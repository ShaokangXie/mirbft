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
	"sync"

	pb "github.com/hyperledger-labs/mirbft/protobufs"
	logger "github.com/rs/zerolog/log"
)

var (
	// 余额表，用内置 map + RWMutex 代替 concurrent-map
	balMu   sync.RWMutex
	balance = make(map[string]float64)

	gasFee = 0.0
	A      = 1
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

	// logger.Debug().Int("AccountCnt", cnt).Msg("Loaded balance !")
	logger.Debug().Int("AccountCnt", len(balance)).Msg("Loaded balance !")
}

// 设置账户余额（覆盖）
func UpdateBalance(accountHash string, amount float64) {
	balMu.Lock()
	balance[accountHash] = amount
	balMu.Unlock()
}

// 读取账户余额；不存在返回 -1
func GetBalance(accountHash string) float64 {
	balMu.RLock()
	v, ok := balance[accountHash]
	balMu.RUnlock()
	if ok {
		return v
	}
	return -1.0
}

// 校验请求是否有足够余额
func RequestIsValid(request *pb.ClientRequest) bool {
	// tx := &pb.Transaction{}
	// if err := proto.Unmarshal(request.Payload, tx); err != nil {
	// 	// 解析失败直接拒绝（也可以按你原来的逻辑返回 true）
	// 	return false
	// }

	// cost := tx.Amount + tx.Fee
	// // 非合约交易需要额外 gas
	// if request.IsContract == 0 {
	// 	cost += gasFee
	// }

	// balMu.RLock()
	// senderBalance, ok := balance[tx.SenderHash]
	// balMu.RUnlock()
	// if !ok {
	// 	// 不存在则视为 0（按你此前语义：返回 true；如果希望严格，可以返回 false）
	// 	return true
	// }
	// return senderBalance >= cost
	return true
}

// 在一个大锁里做“读-改-写”，避免并发条件竞争
func transfer(sender string, receiver string, amount float64) {
	// balMu.Lock()
	// if s, ok := balance[sender]; ok {
	// 	balance[sender] = s - amount
	// }
	// if r, ok := balance[receiver]; ok {
	// 	balance[receiver] = r + amount
	// }
	// balMu.Unlock()
}

// 批量提交请求：
// - 合约交易先冻结 gasFee（先扣掉）
// - 然后无论是否合约，都做 amount+fee 的转账
func CommitEntry(requests []*pb.ClientRequest) {
	// May be concurrent...
	logger.Debug().Int("requestsLen", len(requests)).Msg("account CommitEntry")

	// for _, request := range requests {
	// 	tx := &pb.Transaction{}
	// 	if err := proto.Unmarshal(request.Payload, tx); err != nil {
	// 		continue
	// 	}

	// 	if request.IsContract == 1 {
	// 		// 合约交易先扣除 gas
	// 		balMu.Lock()
	// 		if s, ok := balance[tx.SenderHash]; ok {
	// 			balance[tx.SenderHash] = s - gasFee
	// 		}
	// 		balMu.Unlock()
	// 	}

	// 	// 转账（包含 tx.Fee）
	// 	transfer(tx.SenderHash, tx.ReceiverHash, tx.Amount+tx.Fee)
	// }

	logger.Debug().Float64("Amount", GetBalance("0")).Msg("Account: 0")
}
