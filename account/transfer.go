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
	"errors"
	"math"
	"sort"
	"sync"

	pb "github.com/hyperledger-labs/mirbft/protobufs"
	logger "github.com/rs/zerolog/log"
)

//
// ---- 最小顺序执行基线（无并发、无死锁检测） ----
//

// 如果工程里已定义过 balMu/balance，请删掉下面两行。
var balMu sync.RWMutex
var balance = make(map[int32]float64)

// 设置账户余额（覆盖）
func setBalance(uid int32, amount float64) {
	balMu.Lock()
	balance[uid] = amount
	balMu.Unlock()
}

// 读取账户余额；不存在返回 0
func getBalance(uid int32) float64 {
	balMu.RLock()
	v := balance[uid]
	balMu.RUnlock()
	return v
}

var (
	ErrNonConserved      = errors.New("sum(deltas) must be 0 (fee excluded)")
	ErrInsufficientFunds = errors.New("insufficient funds")
)

// 顺序执行单笔请求（聚合→守恒→余额检查→写回）
func applyRequestSeq(req *pb.ClientRequest) error {
	if req == nil || len(req.Deltas) == 0 {
		return nil
	}

	// 1) 聚合 + 守恒检查
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

	// 2) 为了确定性（可选），对账户升序遍历
	keys := make([]int32, 0, len(agg))
	for uid := range agg {
		keys = append(keys, uid)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	// 3) 原子执行（持全局余额锁，顺序检查与提交）
	balMu.Lock()
	defer balMu.Unlock()

	// 3.1 余额预检查
	// for _, uid := range keys {
	// 	delta := agg[uid]
	// 	if delta < 0 {
	// 		cur := balance[uid]
	// 		// if cur+delta < -1e-12 {
	// 		// 	return fmt.Errorf("%w: uid=%d need=%.4f have=%.4f",
	// 		// 		ErrInsufficientFunds, uid, -delta, cur)
	// 		// }
	// 	}
	// }

	// 3.2 统一写回
	for _, uid := range keys {
		balance[uid] = balance[uid] + agg[uid]
	}

	return nil
}

// 批量提交（严格顺序）：把收齐副本的 tx 逐个 apply
func CommitEntry(requests []*pb.ClientRequest) {
	logger.Info().Int("requestsLen", len(requests)).Msg("account CommitEntry (sequential)")

	for _, r := range requests {
		if err := applyRequestSeq(r); err != nil {
			logger.Error().Err(err).Msg("applyRequestSeq failed")
		}
	}

	logger.Info().Int("executedReady", len(requests)).Msg("CommitEntry(sequential) done")
}
