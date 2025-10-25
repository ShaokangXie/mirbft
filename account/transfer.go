// Copyright 2022 IBM Corp. All Rights Reserved.
// Licensed under the Apache License, Version 2.0

package account

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/hyperledger-labs/mirbft/protobufs"
	logger "github.com/rs/zerolog/log"
)

///////////////////////////
// 余额存储（64 分片）
///////////////////////////

type balShard struct {
	mu sync.RWMutex
	m  map[int32]float64 // 为保持与 pb 兼容，内部仍用 float64
}

// 64 分片，降低写入锁竞争
var balanceShards [64]balShard

func init() {
	for i := range balanceShards {
		balanceShards[i].m = make(map[int32]float64, 1024)
	}
	LoadData()
}

func shardOf(uid int32) *balShard { return &balanceShards[uid&63] }

// 设置账户余额（覆盖）
func setBalance(uid int32, amount float64) {
	s := shardOf(uid)
	s.mu.Lock()
	s.m[uid] = amount
	s.mu.Unlock()
}

// 读取账户余额；不存在返回 0
func getBalance(uid int32) float64 {
	s := shardOf(uid)
	s.mu.RLock()
	v := s.m[uid]
	s.mu.RUnlock()
	return v
}

// 原子地累加余额（持有对象级锁后调用）
func addBalance(uid int32, delta float64) {
	s := shardOf(uid)
	s.mu.Lock()
	s.m[uid] = s.m[uid] + delta
	s.mu.Unlock()
}

func LoadData() {
	// 这里示意性加载几条初始数据；你也可以替换为实际 CSV/DB。
	setBalance(101, 0.0)
	setBalance(202, 0.0)
	setBalance(303, 0.0)

	logger.Debug().Int("AccountCnt", 3).Msg("Loaded balance !")
}

///////////////////////////
// tx 去重（和你现有一致）
///////////////////////////

type txKey = string // 用 RequestID 派生一个稳定 key

type txSeen struct {
	need int32             // 该 tx 应当出现的实例集合（由上层计算）
	seen int32             // 已经在哪些实例看到
	req  *pb.ClientRequest // 最后一份完整请求用于执行
}

var (
	ErrNonConserved      = errors.New("sum(deltas) must be 0 (fee excluded)")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrOverflow          = errors.New("float overflow")

	trkMu   sync.Mutex
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

func UpdateAndCollectReady(reqs []*pb.ClientRequest) []*pb.ClientRequest {
	trkMu.Lock()
	defer trkMu.Unlock()

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
			delete(tracker, key) // 防止二次执行
		}
	}
	return ready
}

///////////////////////////
// 轻量对象锁（CAS 快路径）
///////////////////////////

// 每个对象(userId)一把 CAS 锁：owner=0 表示空闲；否则为 token
type objLock struct {
	owner uint64 // 原子字段
}

var lockTable sync.Map // key=int32(uid) -> *objLock

func getLock(uid int32) *objLock {
	if v, ok := lockTable.Load(uid); ok {
		return v.(*objLock)
	}
	l := &objLock{}
	if actual, _ := lockTable.LoadOrStore(uid, l); actual != nil {
		return actual.(*objLock)
	}
	return l
}

// 稳定 token：基于 txKey 的 FNV-1a 64 位哈希；0 保留
func txOrderHash(k txKey) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(k))
	return h.Sum64()
}
func tokenOf(tk txKey) uint64 {
	t := txOrderHash(tk)
	if t == 0 {
		t = 1
	}
	return t
}

func casAcquire(l *objLock, me uint64) bool {
	return atomic.CompareAndSwapUint64(&l.owner, 0, me)
}
func loadOwner(l *objLock) uint64 {
	return atomic.LoadUint64(&l.owner)
}
func releaseOwner(l *objLock, me uint64) {
	if atomic.LoadUint64(&l.owner) == me {
		atomic.StoreUint64(&l.owner, 0)
	}
}

// 按升序 keys 依次尝试拿锁；失败则释放已拿并返回
func tryClaimAll(tk txKey, keys []int32) (ok bool, acquired []int32) {
	me := tokenOf(tk)
	acquired = make([]int32, 0, len(keys))
	for _, id := range keys {
		l := getLock(id)
		if casAcquire(l, me) {
			acquired = append(acquired, id)
			continue
		}
		owner := loadOwner(l)
		if owner == me {
			continue // 已经持有
		}
		// 冲突：放弃本轮，交给外层退避后重试
		releaseAll(me, acquired)
		return false, nil
	}
	return true, acquired
}

func releaseAll(me uint64, ids []int32) {
	// 逆序释放可略减假共享（不是强要求）
	for i := len(ids) - 1; i >= 0; i-- {
		l := getLock(ids[i])
		releaseOwner(l, me)
	}
}

///////////////////////////
// 执行逻辑
///////////////////////////

func applyRequest(req *pb.ClientRequest) error {
	if req == nil || len(req.Deltas) == 0 {
		return nil
	}

	// 1) 聚合 & 守恒校验（仍是 float64，保持兼容）
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

	// 2) 对象键升序，确保获得锁顺序一致（避免 ABA）
	keys := make([]int32, 0, len(agg))
	for uid := range agg {
		keys = append(keys, uid)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// 3) 确定性微退避（避免抖动，无需队列/通道）
	tk := txDetKey(req)
	me := tokenOf(tk)
	backoff := time.Duration(txOrderHash(tk)%200+50) * time.Microsecond

	for {
		ok, got := tryClaimAll(tk, keys)
		if ok {
			// 可选：余额充足校验（如需），否则直接写回
			// for uid, delta := range agg {
			// 	if delta < 0 && getBalance(uid)+delta < -1e-12 {
			// 		releaseAll(me, got)
			// 		return fmt.Errorf("%w: uid=%d", ErrInsufficientFunds, uid)
			// 	}
			// }
			for uid, delta := range agg {
				addBalance(uid, delta)
			}
			releaseAll(me, got)
			return nil
		}
		// 退避后重试（确定性）
		time.Sleep(backoff)
	}
}

// 批量提交：先去重，后顺序执行（并行度由上层 worker 池控制）
func CommitEntry(requests []*pb.ClientRequest) {
	logger.Debug().
		Int("requestsLen", len(requests)).
		Msg("account CommitEntry (CAS-light)")

	ready := UpdateAndCollectReady(requests)
	if len(ready) == 0 {
		return
	}

	for _, r := range ready {
		if err := applyRequest(r); err != nil {
			logger.Error().Err(err).Msg("applyRequest failed")
		}
	}

	// 示例：读取一个账户看结果
	logger.Info().
		Float64("TotalAmount(uid=101)", getBalance(101)).
		Int("executedReady", len(ready)).
		Msg("CommitEntry done")
}
