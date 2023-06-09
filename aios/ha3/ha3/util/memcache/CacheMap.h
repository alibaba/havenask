/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stdint.h>
#include <stdio.h>
#include <map>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/util/memcache/CacheItem.h"
#include "ha3/util/memcache/atomic.h"

namespace autil {
class ReadWriteLock;
}  // namespace autil

namespace isearch {
namespace util {

#define CACHE_MAX_SIZESTEP 24
extern uint32_t szHashMap_StepTable3[];
typedef CacheItem HashEntry;
typedef HashEntry *HashEntryPtr;

class CCacheMap
{
private:
    u_n32_t getHashSizeStep(u_n32_t nHashSize)
    {
        if (nHashSize < szHashMap_StepTable3[0])
            return 0;
        BinarySearch<u_n32_t> binarySearch;
        return binarySearch(szHashMap_StepTable3,0,CACHE_MAX_SIZESTEP,nHashSize);
    }

private:
    u_n32_t m_nHashSize;
    u_n32_t m_nLockSize;
    u_n32_t m_nUsedSize;
    atomic64_t m_nHashUsed;
    u_n32_t m_nRehashLimit;
    u_n32_t m_nHashSizeStep;
    HashEntry **m_ppEntry;
    autil::ReadWriteLock* m_pBucketLock;
    autil::ReadWriteLock* m_pNodeLock;

public:
    CCacheMap(u_n8_t cHashSizeStep = 0);
    CCacheMap(u_n32_t nHashSize);
    virtual ~CCacheMap(void);
    u_n32_t init(void);

public:
    bool insert(HashEntryPtr item);
    HashEntry *remove(const u_n64_t& _key, const HashEntry *entry);
    void clear(void);
    HashEntryPtr find(const u_n64_t& _key, const HashEntryPtr _none);
    HashEntryPtr lookupEntry(const u_n64_t& _key);
    u_n32_t size() const;
    u_n32_t rehash();
    void print();
    int lockRead(const u_n64_t & _key);
    int lockWrite(const u_n64_t & _key);
    int tryLockRead(const u_n64_t & _key);
    int tryLockWrite(const u_n64_t & _key);
    int unlock(const u_n64_t & _key);
    bool consistencyCheck(std::map<void *, size_t> &hashMap);
private:
    int lockBucket();
    int unlockBucket();

    uint32_t getLockPos(uint64_t key) const;

    AUTIL_LOG_DECLARE();
};

} // namespace util
} // namespace isearch
