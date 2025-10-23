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
	"sort"
	"sync"
)

// 每个 key 的互斥锁 + 引用计数（有人持有就保留，没人持有就从表里删除）
type keyLock struct {
	mu  sync.Mutex
	ref int
}

var (
	// 余额表，用内置 map + RWMutex 代替 concurrent-map
	balMu   sync.RWMutex
	balance = make(map[int32]float64)

	gasFee = 0.0
	A      = 1

	lockTableMu sync.Mutex
	lockTable   = make(map[int32]*keyLock)
)

// KeyGuard 表示已加锁的一组 key；调用 Unlock() 释放
type KeyGuard struct {
	keys []int32
	ls   []*keyLock
}

// LockKey 加锁单个 key
func LockKey(k int32) *KeyGuard {
	return LockKeys(k)
}

// LockKeys 一次性加锁多 key（内部排序去重，避免死锁）
func LockKeys(keys ...int32) *KeyGuard {
	if len(keys) == 0 {
		return &KeyGuard{}
	}
	// 排序 + 去重
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	uniq := keys[:0]
	var last *int32
	for i := range keys {
		if last == nil || keys[i] != *last {
			uniq = append(uniq, keys[i])
			last = &keys[i]
		}
	}
	keys = uniq

	// 取出/创建 keyLock，并 +ref
	ls := make([]*keyLock, len(keys))
	lockTableMu.Lock()
	for i, k := range keys {
		kl := lockTable[k]
		if kl == nil {
			kl = &keyLock{}
			lockTable[k] = kl
		}
		kl.ref++
		ls[i] = kl
	}
	lockTableMu.Unlock()

	// 按相同顺序逐个加互斥，避免环路
	for _, kl := range ls {
		kl.mu.Lock()
	}

	return &KeyGuard{keys: keys, ls: ls}
}

// 解锁（逆序释放），并在无人持有时从表中清理
func (g *KeyGuard) Unlock() {
	if g == nil || len(g.ls) == 0 {
		return
	}
	// 先释放互斥锁
	for i := len(g.ls) - 1; i >= 0; i-- {
		g.ls[i].mu.Unlock()
	}
	// 再减引用并做垃圾回收
	lockTableMu.Lock()
	for i, k := range g.keys {
		kl := g.ls[i]
		kl.ref--
		if kl.ref == 0 {
			delete(lockTable, k)
		}
	}
	lockTableMu.Unlock()
}
