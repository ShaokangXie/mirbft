// Copyright 2022 IBM Corp. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package account

import (
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"time"

	pb "github.com/hyperledger-labs/mirbft/protobufs"
	logger "github.com/rs/zerolog/log"
)

// ------------------------- 余额表（与原工程保持一致的最小接口） -------------------------

// setBalance / getBalance 由 balance.go 提供全局 map 和锁；这里只给出最小实现占位
// 若你已经有 balance.go（含 balMu/balance 等），请删除这两段占位实现，或保持一致。
// var (
// 	balMu   sync.RWMutex
// 	balance = make(map[int32]float64)
// )

func setBalance(uid int32, amount float64) {
	balMu.Lock()
	balance[uid] = amount
	balMu.Unlock()
}

func getBalance(uid int32) float64 {
	balMu.RLock()
	v, ok := balance[uid]

	balMu.RUnlock()
	if !ok {
		return 0.0
	}
	return v
}

// ------------------------- 事务聚合与去重 -------------------------

type txKey = string // 用 RequestID 派生一个稳定 key

type txSeen struct {
	need int32             // 该 tx 需要的副本数
	seen int32             // 已见副本数
	req  *pb.ClientRequest // 持最后一份请求用于执行
}

var (
	ErrNonConserved      = errors.New("sum(deltas) must be 0 (fee excluded)")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrOverflow          = errors.New("float overflow")

	mu      sync.Mutex
	tracker = make(map[txKey]*txSeen)
)

func txDetKey(req *pb.ClientRequest) txKey {
	if rid := req.GetRequestId(); rid != nil {
		return fmt.Sprintf("cid=%d/sn=%d/rep=%d",
			rid.GetClientId(), rid.GetClientSn(), rid.GetClientReplication())
	}
	sum := sha256.Sum256(req.GetPayload())
	return "pl:" + hex.EncodeToString(sum[:8])
}

func txOrderHash(k txKey) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(k))
	return h.Sum64()
}
func lessTxKey(a, b txKey) bool {
	ha, hb := txOrderHash(a), txOrderHash(b)
	if ha == hb {
		return a < b
	}
	return ha < hb
}

// 将本 entry 的请求并到全局计数，收齐副本的返回执行
func UpdateAndCollectReady(reqs []*pb.ClientRequest) []*pb.ClientRequest {
	mu.Lock()
	defer mu.Unlock()

	ready := make([]*pb.ClientRequest, 0, len(reqs))
	for _, req := range reqs {
		if req == nil || len(req.Deltas) == 0 {
			continue
		}
		key := txDetKey(req)
		ts := tracker[key]
		if ts == nil {
			ts = &txSeen{
				need: req.RequestId.ClientReplication,
				seen: 0,
				req:  req,
			}
			tracker[key] = ts
		}
		ts.seen++
		if ts.seen >= ts.need {
			ready = append(ready, ts.req)
			delete(tracker, key)
		}
	}
	return ready
}

// ------------------------- 轻量锁管理（2-cycle 检测） -------------------------

type objLock struct {
	owner txKey
	waitQ *txMinHeap // 按 lessTxKey 的确定性顺序
}
type txMinHeap []txKey

func (h txMinHeap) Len() int           { return len(h) }
func (h txMinHeap) Less(i, j int) bool { return lessTxKey(h[i], h[j]) }
func (h txMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *txMinHeap) Push(x any)        { *h = append(*h, x.(txKey)) }
func (h *txMinHeap) Pop() any          { old := *h; x := old[len(old)-1]; *h = old[:len(old)-1]; return x }

type engine struct {
	mu      sync.Mutex
	locks   map[int32]*objLock           // obj -> 锁
	owners  map[txKey]map[int32]struct{} // tx -> 已持有对象
	waiting map[txKey]map[int32]struct{} // tx -> 正在等待对象（反向索引）

	waitCh map[txKey]chan struct{} // tx -> 等待通道（定向唤醒）
}

var eng = &engine{
	locks:   make(map[int32]*objLock),
	owners:  make(map[txKey]map[int32]struct{}),
	waiting: make(map[txKey]map[int32]struct{}),
	waitCh:  make(map[txKey]chan struct{}),
}

func (e *engine) ensureWaitChLocked(tk txKey) chan struct{} {
	ch := e.waitCh[tk]
	if ch == nil {
		ch = make(chan struct{}, 1)
		e.waitCh[tk] = ch
	}
	return ch
}
func (e *engine) ensureWaitCh(tk txKey) chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ensureWaitChLocked(tk)
}
func (e *engine) notifyUnsafe(tk txKey) {
	if ch, ok := e.waitCh[tk]; ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
func removeFromWaitQ(h *txMinHeap, tk txKey) bool {
	for i, v := range *h {
		if v == tk {
			heap.Remove(h, i)
			return true
		}
	}
	return false
}

// 回滚本次 tryLockAll 已拿到的部分对象（需持有 e.mu）
func (e *engine) releaseSubsetLocked(tk txKey, objs []int32) {
	for _, obj := range objs {
		ol := e.locks[obj]
		if ol == nil || ol.owner != tk {
			continue
		}
		if ol.waitQ.Len() > 0 {
			next := heap.Pop(ol.waitQ).(txKey)
			ol.owner = next
			if _, ok := e.owners[next]; !ok {
				e.owners[next] = make(map[int32]struct{})
			}
			e.owners[next][obj] = struct{}{}
			if w, ok := e.waiting[next]; ok {
				delete(w, obj)
				if len(w) == 0 {
					delete(e.waiting, next)
				}
			}
			e.notifyUnsafe(next)
		} else {
			ol.owner = ""
		}
		delete(e.owners[tk], obj)
	}
}

// 2-cycle 快检：owner 是否等待着 tk 正持有的任意对象？
func (e *engine) hasTwoCycleLocked(tk, owner txKey) bool {
	waits := e.waiting[owner]
	if waits == nil {
		return false
	}
	for obj := range waits {
		if ol := e.locks[obj]; ol != nil && ol.owner == tk {
			return true
		}
	}
	return false
}

// 尝试拿齐 keys；若遇冲突，先做 2-cycle 快检，若自己是受害者则“放弃本次并回滚”。
// 返回 (allAcquired, abortedSelf)
func (e *engine) tryLockAll(tk txKey, keys []int32) (bool, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.owners[tk]; !ok {
		e.owners[tk] = make(map[int32]struct{}, len(keys))
	}
	if _, ok := e.waiting[tk]; !ok {
		e.waiting[tk] = make(map[int32]struct{})
	}

	acquiredThisCall := make([]int32, 0, len(keys))
	all := true

	for _, obj := range keys {
		if _, ok := e.owners[tk][obj]; ok {
			continue
		}
		ol := e.locks[obj]
		if ol == nil {
			ol = &objLock{waitQ: &txMinHeap{}}
			heap.Init(ol.waitQ)
			e.locks[obj] = ol
		}
		if ol.owner == "" {
			ol.owner = tk
			e.owners[tk][obj] = struct{}{}
			acquiredThisCall = append(acquiredThisCall, obj)
			delete(e.waiting[tk], obj)
			continue
		}
		if ol.owner == tk {
			continue
		}

		owner := ol.owner

		// ---- 2-cycle 快检：tk <-> owner 互等？ ----
		if e.hasTwoCycleLocked(tk, owner) {
			// 选受害者（确定性）：用 lessTxKey；更“年轻”的作为受害者
			victim := owner
			if lessTxKey(owner, tk) {
				victim = tk
			}
			if victim == tk {
				// 自己为受害者：回滚本次已拿到的锁，清理等待登记，放弃本次
				e.releaseSubsetLocked(tk, acquiredThisCall)
				if waits := e.waiting[tk]; waits != nil {
					for o := range waits {
						if l := e.locks[o]; l != nil && l.waitQ != nil {
							removeFromWaitQ(l.waitQ, tk)
						}
					}
					delete(e.waiting, tk)
				}
				return false, true // abortedSelf
			}
			// 对方为受害者：这里不去“强制剥夺”，保持 tk 正常排队等待，由对方未来释放/被外层重试处理
		}

		// 正常排队等待（去重）
		if _, already := e.waiting[tk][obj]; !already {
			heap.Push(ol.waitQ, tk)
			e.waiting[tk][obj] = struct{}{}
			e.ensureWaitChLocked(tk)
		}
		all = false
	}
	return all, false
}

func (e *engine) releaseAll(tk txKey) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 释放持有的对象
	if owned := e.owners[tk]; owned != nil {
		for obj := range owned {
			if ol := e.locks[obj]; ol != nil && ol.owner == tk {
				if ol.waitQ.Len() > 0 {
					next := heap.Pop(ol.waitQ).(txKey)
					ol.owner = next
					if _, ok := e.owners[next]; !ok {
						e.owners[next] = make(map[int32]struct{})
					}
					e.owners[next][obj] = struct{}{}
					if w, ok := e.waiting[next]; ok {
						delete(w, obj)
						if len(w) == 0 {
							delete(e.waiting, next)
						}
					}
					e.notifyUnsafe(next)
				} else {
					ol.owner = ""
				}
			}
		}
	}

	// 把 tk 从仍在等待的对象的队列中移除（防“僵尸等待者”）
	if waits := e.waiting[tk]; waits != nil {
		for obj := range waits {
			if ol := e.locks[obj]; ol != nil && ol.waitQ != nil {
				removeFromWaitQ(ol.waitQ, tk)
			}
		}
	}

	delete(e.owners, tk)
	delete(e.waiting, tk)
	delete(e.waitCh, tk)
}

// ------------------------- 执行路径 -------------------------

func applyRequest(req *pb.ClientRequest) error {
	if req == nil || len(req.Deltas) == 0 {
		return nil
	}

	// 聚合 + 守恒
	agg := make(map[int32]float64, len(req.Deltas))
	var sum float64
	for _, d := range req.Deltas {
		if d == nil || d.AmountDelta == 0 {
			continue
		}
		agg[d.UserId] += d.AmountDelta
		sum += d.AmountDelta
	}
	if math.Abs(sum) > 1e-12 {
		return ErrNonConserved
	}

	// 事务键 + 排序后的对象列表
	tk := txDetKey(req)
	keys := make([]int32, 0, len(agg))
	for uid := range agg {
		keys = append(keys, uid)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// 拿齐锁；若未拿到且未被判为受害者，则等待定向唤醒并重试
	for {
		all, aborted := eng.tryLockAll(tk, keys)
		if aborted {
			return errors.New("aborted by 2-cycle resolver")
		}
		if all {
			break
		}
		ch := eng.ensureWaitCh(tk)
		select {
		case <-ch:
		case <-time.After(2 * time.Millisecond):
		}
	}

	// 执行（此处示例为直接写回；如需余额检查可解注下段）
	// for uid, delta := range agg {
	// 	if delta < 0 {
	// 		if cur := getBalance(uid); cur+delta < -1e-12 {
	// 			eng.releaseAll(tk)
	// 			return fmt.Errorf("%w: uid=%d need=%f have=%f", ErrInsufficientFunds, uid, -delta, cur)
	// 		}
	// 	}
	// }
	for uid, delta := range agg {
		cur := getBalance(uid)
		setBalance(uid, cur+delta)
	}
	eng.releaseAll(tk)
	return nil
}

// 批量提交（顺序执行，系统并行度由上一层 worker 池控制）
func CommitEntry(requests []*pb.ClientRequest) {
	ready := UpdateAndCollectReady(requests)
	if len(ready) == 0 {
		return
	}
	for _, r := range ready {
		if err := applyRequest(r); err != nil {
			logger.Error().Err(err).Msg("applyRequest failed")
		}
	}
	logger.Info().Int("executedReady", len(ready)).Msg("CommitEntryWithInstance done")
}
