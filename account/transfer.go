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

func init() {
	// if tmpNum, err := strconv.ParseFloat(config.Config.Gasfee, 64); err == nil {
	// 	logger.Debug().Float64("Gasfee", tmpNum).Msg("Gas Fee.")
	// 	gasFee = tmpNum
	// }
	LoadData()
	logger.Debug().Int("a", A).Msg("In balance init() !")
}

func LoadData() {
	// cnt := 0

	// homedir, _ := os.UserHomeDir()
	// file, err := os.Open(homedir + "/balance.csv")
	// if err != nil {
	// 	panic(err)
	// }
	// defer file.Close()

	// br := bufio.NewReader(file)
	// for {
	// 	cnt++
	// 	line, _, c := br.ReadLine()
	// 	if c == io.EOF {
	// 		break
	// 	}
	// 	fields := strings.Split(string(line), ",")
	// 	if len(fields) < 2 {
	// 		continue
	// 	}
	// 	amt, err := strconv.ParseFloat(fields[1], 64)
	// 	if err != nil {
	// 		logger.Fatal().Msg(err.Error())
	// 	}
	// 	UpdateBalance(fields[0], amt)
	// }

	setBalance(101, 0.0)
	setBalance(202, 0.0)
	setBalance(303, 0.0)

	// logger.Debug().Int("AccountCnt", cnt).Msg("Loaded balance !")
	logger.Debug().Int("AccountCnt", len(balance)).Msg("Loaded balance !")
}

// 设置账户余额（覆盖）
func setBalance(uid int32, amount float64) {
	balMu.Lock()
	balance[uid] = amount
	balMu.Unlock()
}

// 读取账户余额；不存在返回 -1
func getBalance(uid int32) float64 {
	balMu.RLock()
	v, ok := balance[uid]
	balMu.RUnlock()
	if !ok {
		return 0.0
	}
	return v
}

type txKey = string // 用 RequestID 派生一个稳定 key

type txSeen struct {
	need int32             // 该 tx 应当出现的实例集合（位图），靠对象→桶计算
	seen int32             // 已经在哪些实例看到
	req  *pb.ClientRequest // 可持最后一份完整请求用于执行
}

var (
	ErrNonConserved      = errors.New("sum(deltas) must be 0 (fee excluded)")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrOverflow          = errors.New("float overflow")
	mu                   sync.Mutex
	tracker              = make(map[txKey]*txSeen)
)

func txDetKey(req *pb.ClientRequest) txKey {
	// 用 RequestID 三元组生成稳定 key（和上一版一致）
	if rid := req.GetRequestId(); rid != nil {
		return fmt.Sprintf("cid=%d/sn=%d/rep=%d",
			rid.GetClientId(), rid.GetClientSn(), rid.GetClientReplication())
	}
	// 兜底：payload 哈希
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

func UpdateAndCollectReady(reqs []*pb.ClientRequest) []*pb.ClientRequest {
	mu.Lock()
	defer mu.Unlock()

	ready := make([]*pb.ClientRequest, 0)

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

		// 达到应当集合 ⇒ ready
		if ts.seen >= ts.need {
			ready = append(ready, ts.req)
			// 防止重复执行：可以直接删除或转移到“已执行”集合
			delete(tracker, key)
		}
	}
	return ready
}

type objLock struct {
	owner txKey
	waitQ *txMinHeap
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
	waiting map[txKey]map[int32]struct{} // tx -> 正在等待对象

	waitCh map[txKey]chan struct{} // tx -> 等待通道（定向唤醒）
}

var eng = &engine{
	locks:   make(map[int32]*objLock),
	owners:  make(map[txKey]map[int32]struct{}),
	waiting: make(map[txKey]map[int32]struct{}),
	waitCh:  make(map[txKey]chan struct{}),
}

// 仅在持有 e.mu 的情况下调用：确保存在等待通道
func (e *engine) ensureWaitChLocked(tk txKey) chan struct{} {
	ch := e.waitCh[tk]
	if ch == nil {
		ch = make(chan struct{}, 1) // 缓冲 1，保证唤醒不阻塞
		e.waitCh[tk] = ch
	}
	return ch
}

// 在未持锁的情况下也可用
func (e *engine) ensureWaitCh(tk txKey) chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ensureWaitChLocked(tk)
}

// 仅在持有 e.mu 时调用：非阻塞唤醒一个等待者
func (e *engine) notifyUnsafe(tk txKey) {
	if ch, ok := e.waitCh[tk]; ok {
		select {
		case ch <- struct{}{}: // 成功投递唤醒信号
		default: // 已有未消费的信号，跳过
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

func (e *engine) tryLockAll(tk txKey, keys []int32) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.owners[tk]; !ok {
		e.owners[tk] = make(map[int32]struct{}, len(keys))
	}
	if _, ok := e.waiting[tk]; !ok {
		e.waiting[tk] = make(map[int32]struct{})
	}

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
			delete(e.waiting[tk], obj)
		} else if ol.owner != tk {
			if _, already := e.waiting[tk][obj]; !already {
				heap.Push(ol.waitQ, tk)
				e.waiting[tk][obj] = struct{}{}
				e.ensureWaitChLocked(tk) // 确保有专属唤醒 chan
			}
			all = false
		}
	}
	return all
}

func (e *engine) releaseAll(tk txKey) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1) 释放已持有的对象
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

	// 2) 清理仍在等待的对象队列中的 tk（关键修复）
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

func (e *engine) detectDeadlockPickVictim() (victim txKey) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 构建等待图：u 等待 obj，且 obj 的 owner 是 v => u -> v
	wfg := make(map[txKey]map[txKey]struct{})
	nodes := make(map[txKey]struct{})

	addEdge := func(u, v txKey) {
		if u == "" || v == "" || u == v {
			return
		}
		if _, ok := wfg[u]; !ok {
			wfg[u] = make(map[txKey]struct{})
		}
		wfg[u][v] = struct{}{}
		nodes[u], nodes[v] = struct{}{}, struct{}{}
	}

	for u, waits := range e.waiting {
		for obj := range waits {
			if ol := e.locks[obj]; ol != nil && ol.owner != "" && ol.owner != u {
				addEdge(u, ol.owner)
			}
		}
	}

	// 将节点列表按确定性顺序排序
	nodeList := make([]txKey, 0, len(nodes))
	for u := range nodes {
		nodeList = append(nodeList, u)
	}
	sort.Slice(nodeList, func(i, j int) bool { return lessTxKey(nodeList[i], nodeList[j]) })

	// 邻居获取：对每个节点的邻接点也按确定性顺序排序
	neighbors := func(u txKey) []txKey {
		m := wfg[u]
		if m == nil {
			return nil
		}
		out := make([]txKey, 0, len(m))
		for v := range m {
			out = append(out, v)
		}
		sort.Slice(out, func(i, j int) bool { return lessTxKey(out[i], out[j]) })
		return out
	}

	visited := make(map[txKey]bool)
	onStack := make(map[txKey]bool)
	stack := make([]txKey, 0)

	// DFS：一旦找到环，选择环内哈希最小的 tx 作为受害者
	var found bool
	var dfs func(txKey) bool
	dfs = func(u txKey) bool {
		visited[u], onStack[u] = true, true
		stack = append(stack, u)

		for _, v := range neighbors(u) {
			if !visited[v] {
				if dfs(v) {
					return true
				}
			} else if onStack[v] {
				// 发现回边，stack 中 v..u 构成一个环
				cycle := collectCycle(stack, v)
				min := cycle[0]
				for _, x := range cycle[1:] {
					if lessTxKey(x, min) {
						min = x
					}
				}
				victim = min
				found = true
				return true
			}
		}

		// 回溯
		stack = stack[:len(stack)-1]
		onStack[u] = false
		return false
	}

	for _, u := range nodeList {
		if !visited[u] {
			if dfs(u) {
				break
			}
		}
	}

	if found {
		return victim
	}
	return ""
}

func collectCycle(stack []txKey, start txKey) []txKey {
	for i := len(stack) - 1; i >= 0; i-- {
		if stack[i] == start {
			return append([]txKey(nil), stack[i:]...)
		}
	}
	return nil
}

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

	// 事务ID & 对象键（升序）
	tk := txDetKey(req)
	keys := make([]int32, 0, len(agg))
	for uid := range agg {
		keys = append(keys, uid)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// 2PL: 拿齐锁；拿不到则：先尝试解环，否则等待定向唤醒或超时
	for {
		if eng.tryLockAll(tk, keys) {
			break
		}

		// 先尝试一次死锁检测/解环
		if victim := eng.detectDeadlockPickVictim(); victim != "" {
			if victim == tk {
				eng.releaseAll(tk)
				return errors.New("aborted by deadlock resolver")
			}
			eng.releaseAll(victim) // 打破环
			continue
		}

		// 没环：等待“对象锁转交到自己时”的定向唤醒；若超时则重试
		ch := eng.ensureWaitCh(tk)
		select {
		case <-ch:
			// 被唤醒（某把锁已转交给我），立即重试拿齐所有锁
		case <-time.After(2 * time.Millisecond):
			// 兜底超时：避免错过唤醒/或长时间无进展，回去重试 + 可能再次检测
		}
	}

	// // 执行（余额预检查 + 统一写回）
	// for uid, delta := range agg {
	// 	if delta < 0 {
	// 		cur := getBalance(uid)
	// 		if cur+delta < -1e-12 {
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

// 批量提交
func CommitEntry(requests []*pb.ClientRequest) {
	logger.Debug().
		Int("requestsLen", len(requests)).
		Msg("account CommitEntryWithInstance")

	// 第一步：更新 ready（按实例去重）
	ready := UpdateAndCollectReady(requests)
	if len(ready) == 0 {
		return
	}

	// 第二步：顺序执行（并行度由 log 包的上层 worker 池统一控制）
	for _, r := range ready {
		if err := applyRequest(r); err != nil {
			logger.Error().Err(err).Msg("applyRequest failed")
		}
	}

	logger.Info().
		Float64("TotalAmount", getBalance(101)).
		Int("executedReady", len(ready)).
		Msg("CommitEntryWithInstance done")

}
