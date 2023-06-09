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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <new>

#include "autil/FixedSizeAllocator.h"
#include "autil/Lock.h"

namespace autil {

template <class Key, class Value>
class ReferencedHashMap
{
public:
    typedef Value EntryData;
    struct  HashEntry
    {
        EntryData data;
        HashEntry* hashNext;

        ~HashEntry() {
            hashNext = NULL;
        }
    };

public:
    ReferencedHashMap();
    ~ReferencedHashMap();

private:
    ReferencedHashMap(const ReferencedHashMap &);
    ReferencedHashMap& operator = (const ReferencedHashMap &);

public:
    bool init(uint32_t bucketCount, uint32_t lockCount);
   
    inline bool tryDelete(EntryData entryData);//only delete the node of which reference count is zero
    inline EntryData find(Key key);
    inline EntryData findAndInsert(EntryData entryData);

    inline size_t size() const;

private:
    inline EntryData findLockFree(uint32_t entryOffset, Key key) const;
    inline bool deleteEntry(uint32_t entryOffset, EntryData entryData) ;
    size_t adjustBucketNum(size_t n) const;
    void clear();

    HashEntry* allocHashEntry();
    void freeHashEntry(HashEntry *entry);

private:
    autil::ReadWriteLock *_entryLock;
    HashEntry **_buckets;
    uint32_t _lockCount;
    uint32_t _bucketCount;

    size_t _elementCount;

    ThreadMutex _hashEntryAllocatorLock;
    FixedSizeAllocator _hashEntryAllocator;

    friend class ReferencedHashMapTest;
};

/////////////////////////////////////////////////////////////

template <class Key, class Value>
inline ReferencedHashMap<Key, Value>::ReferencedHashMap() 
    : _hashEntryAllocator(sizeof(HashEntry))
{ 
    _entryLock = NULL;
    _buckets = NULL;
    _lockCount = 0;
    _bucketCount = 0;

    _elementCount = 0;
}

template <class Key, class Value>
inline ReferencedHashMap<Key, Value>::~ReferencedHashMap() 
{ 
    clear();
}

template <class Key, class Value>
inline bool ReferencedHashMap<Key, Value>::tryDelete(EntryData entryData) 
{
    assert(_bucketCount >0 && _lockCount > 0
           && NULL != _entryLock && NULL != _buckets);

    if (NULL == entryData) {
        return false;
    }
    uint32_t entryOffset = entryData->getKey() % _bucketCount;
    uint32_t lockOffset = entryOffset % _lockCount;
    bool ret = false;
    ScopedWriteLock lock(_entryLock[lockOffset]);
    ret = (entryData->getRefCount() == 0);
    if (ret) 
    {
        ret = deleteEntry(entryOffset, entryData);
    }

    return ret;
}

template <class Key, class Value>
inline bool ReferencedHashMap<Key, Value>::deleteEntry(
        uint32_t entryOffset, EntryData entryData) 
{
    HashEntry* cur = _buckets[entryOffset];
    HashEntry* prev = NULL;
    while(cur && cur->data->getKey() != entryData->getKey()) 
    {
        prev = cur;
        cur = cur->hashNext;
    }
    
    if (cur) 
    {
        if (NULL == prev) 
        {
            _buckets[entryOffset] = cur->hashNext;    
        } 
        else 
        {
            prev->hashNext = cur->hashNext;
        }
        freeHashEntry(cur);
        __sync_sub_and_fetch(&_elementCount, (uint64_t)1);
        return true;
    }

    return false;
}

template <class Key, class Value>
inline typename ReferencedHashMap<Key, Value>::EntryData 
ReferencedHashMap<Key, Value>::find(Key key) 
{
    assert(_bucketCount > 0 && _lockCount > 0 
           && NULL != _entryLock && NULL != _buckets);

    uint32_t entryOffset = key % _bucketCount;
    uint32_t lockOffset = entryOffset % _lockCount;
    ScopedReadLock lock(_entryLock[lockOffset]);
    EntryData entryData = findLockFree(entryOffset, key);
    if (NULL != entryData) 
    {
        entryData->incRefCount();
    }
    return entryData;
}

template <class Key, class Value>
inline typename ReferencedHashMap<Key, Value>::EntryData 
ReferencedHashMap<Key, Value>::findAndInsert(EntryData entryData) 
{
    assert(_bucketCount > 0 && _lockCount > 0 
           && NULL != _entryLock && NULL != _buckets);

    if (NULL == entryData) 
    {
        return NULL;
    }
    uint32_t entryOffset = entryData->getKey() % _bucketCount;
    uint32_t lockOffset = entryOffset % _lockCount;

    ScopedWriteLock lock(_entryLock[lockOffset]);
    EntryData findEntryData = findLockFree(entryOffset, entryData->getKey());
    if (NULL == findEntryData) 
    {
        HashEntry* newEntry = allocHashEntry();
        newEntry->data = entryData;
        newEntry->hashNext = _buckets[entryOffset];

        _buckets[entryOffset] = newEntry;
        __sync_add_and_fetch(&_elementCount, (uint64_t)1);
        entryData->incRefCount();
        return entryData;
    }
    else 
    {
        findEntryData->incRefCount();
        return findEntryData;
    }
}

template <class Key, class Value>
inline typename ReferencedHashMap<Key, Value>::EntryData 
ReferencedHashMap<Key, Value>::findLockFree(
        uint32_t entryOffset, Key key) const
{
    HashEntry* cur = _buckets[entryOffset];
    while(cur && cur->data->getKey() != key) 
    {
        cur = cur->hashNext;
    }
    return cur ? cur->data : NULL;
}

template <class Key, class Value>
inline size_t ReferencedHashMap<Key, Value>::size() const {
    return _elementCount;
}

template <class Key, class Value>
inline bool ReferencedHashMap<Key, Value>::init(uint32_t bucketCount, uint32_t lockCount) {
    if(_buckets) 
    {
        return false;
    }

    _bucketCount = adjustBucketNum(bucketCount);

    _buckets = new (std::nothrow) HashEntry*[_bucketCount];
    if (!_buckets) 
    {
        return false;
    }
    memset(_buckets, 0, _bucketCount*sizeof(HashEntry*));

    if (lockCount == 0) 
    {
        _lockCount = _bucketCount < 1024 ? _bucketCount : 1024;
    }
    else 
    {
        _lockCount = lockCount;
    }

    _entryLock = new (std::nothrow) autil::ReadWriteLock[_lockCount];
    if (!_entryLock) 
    {
        delete [] _buckets;
        _buckets = NULL;
        return false;
    }
    return true;
}

template <class Key, class Value>
inline size_t ReferencedHashMap<Key, Value>::adjustBucketNum(size_t n) const{
    static const size_t prime_list[] = {
        5ul, 11ul, 17ul, 29ul, 37ul, 53ul, 67ul, 79ul,
        97ul, 131ul, 193ul, 257ul, 389ul, 521ul, 769ul,
        1031ul, 1543ul, 2053ul, 3079ul, 6151ul, 12289ul, 24593ul,
        49157ul, 98317ul, 196613ul, 393241ul, 786433ul,
        1572869ul, 3145739ul, 6291469ul, 12582917ul, 25165843ul,
        50331653ul, 100663319ul, 201326611ul, 402653189ul, 805306457ul,
        1610612741ul, 3221225473ul, 4294967291ul };

    static const size_t length = 
        sizeof(prime_list) / sizeof(size_t);
    
    const size_t *bound =
        std::lower_bound(prime_list, prime_list + length - 1, n);

    return *bound;
}

template <class Key, class Value>
void ReferencedHashMap<Key, Value>::clear()
{
    if (_entryLock) 
    {
        delete [] _entryLock;
        _entryLock = NULL;
    }
    
    for (uint32_t i = 0; i < _bucketCount; ++i)
    {
        HashEntry* entry = _buckets[i];
        while (entry != NULL)
        {
            HashEntry* nextEntry = entry->hashNext;
            freeHashEntry(entry);
            entry = nextEntry;
        }
    }
    if (_buckets) 
    {
        delete [] _buckets;
        _buckets = NULL;
    }
    _hashEntryAllocator.clear();
}

template <class Key, class Value>
inline typename ReferencedHashMap<Key, Value>::HashEntry* 
ReferencedHashMap<Key, Value>::allocHashEntry() {
    ScopedLock lock(_hashEntryAllocatorLock);
    return new (_hashEntryAllocator.allocate()) HashEntry();
}

template <class Key, class Value>
inline void ReferencedHashMap<Key, Value>::freeHashEntry(HashEntry *entry) {
    ScopedLock lock(_hashEntryAllocatorLock);
    entry->~HashEntry();
    _hashEntryAllocator.free(entry);
}

}

