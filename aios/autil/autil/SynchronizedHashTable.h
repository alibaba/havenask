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

#include <algorithm>
#include <stddef.h>
#include <stdint.h>

#include "autil/HashUtil.h"
#include "autil/Lock.h"

namespace autil {

size_t nextBucketNum(size_t n);

template <class K, class V>
struct HashNode {
    K key;
    V val;
    HashNode<K, V> *next;
    HashNode(const K &k, const V &v) : key(k), val(v), next(NULL) {}
};

template <class K, class V>
class HashEntry {
public:
    HashEntry() : _node(NULL) {}

    ~HashEntry();

    inline bool bucketIsUsed() const { return _node != NULL; }

    inline HashNode<K, V> *getNode() const { return _node; }
    inline void setNode(HashNode<K, V> *&node) { _node = node; }

    /*
      If key already exists, overwrite old value, return false
    */
    bool put(const K &key, const V &val);

    /*
      If key already exists, return old value to oldVal parameter, return false
    */
    bool putGet(const K &key, const V &val, V &oldVal, V *&pretVal);

    bool get(const K &key, V &val) const;

    bool getForUpdate(const K &key, V *&pval) const;

    bool deleteNode(const K &key);

    /* not used at current */
    // bool getForDelete(const K& key, HashNode<K, V>*& retNode);

    bool deleteGet(const K &key, V &val);

    void release();

private:
    HashNode<K, V> *_node;
};

template <class K, class V>
HashEntry<K, V>::~HashEntry() {
    release();
}

template <class K, class V>
void HashEntry<K, V>::release() {
    HashNode<K, V> *p = this->_node;
    while (p) {
        HashNode<K, V> *next = p->next;
        delete p;
        p = next;
    }
}

template <class K, class V>
bool HashEntry<K, V>::put(const K &key, const V &val) {
    HashNode<K, V> *pnode = _node;
    HashNode<K, V> *prev = NULL;
    while (pnode) {
        if (key == pnode->key) {
            // overwrite existing value
            pnode->val = val;
            // std::cout << "overwrite key:" << key << std::endl;
            return false;
        } else {
            prev = pnode;
            pnode = pnode->next;
        }
    }
    if (prev != NULL) {
        prev->next = new HashNode<K, V>(key, val);
        // std::cout << "insert conflict key:" << key << std::endl;
    } else {
        _node = new HashNode<K, V>(key, val);
        // std::cout << "insert first key:" << key << std::endl;
    }
    return true;
}

template <class K, class V>
bool HashEntry<K, V>::putGet(const K &key, const V &val, V &oldVal, V *&pretVal) {
    HashNode<K, V> *pnode = _node;
    HashNode<K, V> *prev = NULL;
    while (pnode) {
        if (key == pnode->key) {
            // overwrite existing value
            oldVal = pnode->val;
            pnode->val = val;
            pretVal = &pnode->val;
            // std::cout << "overwrite key:" << key << std::endl;
            return false;
        } else {
            prev = pnode;
            pnode = pnode->next;
        }
    }
    if (prev != NULL) {
        prev->next = new HashNode<K, V>(key, val);
        pretVal = &prev->next->val;
        // std::cout << "insert conflict key:" << key << std::endl;
    } else {
        _node = new HashNode<K, V>(key, val);
        pretVal = &_node->val;
        // std::cout << "insert first key:" << key << std::endl;
    }
    return true;
}

template <class K, class V>
bool HashEntry<K, V>::get(const K &key, V &val) const {
    HashNode<K, V> *pnode = _node;
    while (pnode) {
        if (key == pnode->key) {
            val = pnode->val;
            return true;
        } else {
            pnode = pnode->next;
        }
    }
    return false;
}

template <class K, class V>
bool HashEntry<K, V>::getForUpdate(const K &key, V *&pval) const {
    HashNode<K, V> *pnode = _node;
    while (pnode) {
        if (key == pnode->key) {
            pval = &pnode->val;
            return true;
        } else {
            pnode = pnode->next;
        }
    }
    pval = NULL;
    return false;
}

template <class K, class V>
bool HashEntry<K, V>::deleteNode(const K &key) {
    HashNode<K, V> *pnode = _node;
    HashNode<K, V> *prev = NULL;
    while (pnode) {
        if (key == pnode->key) {
            if (prev == NULL) {
                HashNode<K, V> *follow = _node->next;
                delete _node;
                _node = follow;
            } else {
                prev->next = pnode->next;
                delete pnode;
            }
            return true;
        } else {
            prev = pnode;
            pnode = pnode->next;
        }
    }
    return false;
}

template <class K, class V>
bool HashEntry<K, V>::deleteGet(const K &key, V &val) {
    HashNode<K, V> *pnode = _node;
    HashNode<K, V> *prev = NULL;
    while (pnode) {
        if (key == pnode->key) {
            if (prev == NULL) {
                val = _node->val;
                HashNode<K, V> *follow = _node->next;
                delete _node;
                _node = follow;
            } else {
                val = pnode->val;
                prev->next = pnode->next;
                delete pnode;
            }
            return true;
        } else {
            prev = pnode;
            pnode = pnode->next;
        }
    }
    return false;
}

template <class K, class V, class HashFunc = KeyHash<K>>
class SynchronizedHashTable {
public:
    static const uint32_t BUCKET_NUM_DEF = 1 << 23;
    static const uint32_t LOCK_NUM_DEF = 1 << 10;

public:
    SynchronizedHashTable(uint32_t bucketNumber = BUCKET_NUM_DEF, HashFunc hasher = KeyHash<K>());
    ~SynchronizedHashTable();

public:
    HashEntry<K, V> *getHashEntry(const K &key, ReadWriteLock *&pLock) const;

    /*
      If key already exists, overwrite old value, return false
    */
    bool put(const K &key, const V &val);

    bool get(const K &key, V &val) const;

    bool deleteNode(const K &key);

    bool deleteGet(const K &key, V &val);

    /* not used at current */
    /* after call this method, releaseNode() must be called to by caller */
    // bool getForDelete(const K& key, HashNode<K, V>*& node);
    void releaseNode(HashNode<K, V> *&node);

private:
    HashEntry<K, V> *_buckets;
    ReadWriteLock *_locks;

    uint32_t _bucketsSize;
    uint32_t _locksSize;

    HashFunc _hasher;
};

template <class K, class V, class HashFunc>
SynchronizedHashTable<K, V, HashFunc>::SynchronizedHashTable(uint32_t bucketNumber, HashFunc hasher) : _hasher(hasher) {
    _bucketsSize = nextBucketNum(bucketNumber);
    _buckets = new HashEntry<K, V>[_bucketsSize];

    _locksSize = LOCK_NUM_DEF;
    _locks = new ReadWriteLock[_locksSize];
}

template <class K, class V, class HashFunc>
SynchronizedHashTable<K, V, HashFunc>::~SynchronizedHashTable() {
    delete[] _buckets;
    delete[] _locks;
}

template <class K, class V, class HashFunc>
HashEntry<K, V> *SynchronizedHashTable<K, V, HashFunc>::getHashEntry(const K &key, ReadWriteLock *&pLock) const {
    uint64_t k = _hasher(key);
    uint64_t offset = k % _bucketsSize;
    uint64_t lockOffset = offset % _locksSize;
    pLock = _locks + lockOffset;
    return _buckets + offset;
}

template <class K, class V, class HashFunc>
bool SynchronizedHashTable<K, V, HashFunc>::put(const K &key, const V &val) {
    ReadWriteLock *rwlock = NULL;
    HashEntry<K, V> *entry = getHashEntry(key, rwlock);
    ScopedWriteLock lock(*rwlock);
    return entry->put(key, val);
}

template <class K, class V, class HashFunc>
bool SynchronizedHashTable<K, V, HashFunc>::get(const K &key, V &val) const {
    ReadWriteLock *rwlock = NULL;
    HashEntry<K, V> *entry = getHashEntry(key, rwlock);
    ScopedReadLock lock(*rwlock);
    return entry->get(key, val);
}

template <class K, class V, class HashFunc>
bool SynchronizedHashTable<K, V, HashFunc>::deleteNode(const K &key) {
    ReadWriteLock *rwlock = NULL;
    HashEntry<K, V> *entry = getHashEntry(key, rwlock);
    ScopedWriteLock lock(*rwlock);
    return entry->deleteNode(key);
}

template <class K, class V, class HashFunc>
bool SynchronizedHashTable<K, V, HashFunc>::deleteGet(const K &key, V &val) {
    ReadWriteLock *rwlock = NULL;
    HashEntry<K, V> *entry = getHashEntry(key, rwlock);
    ScopedWriteLock lock(*rwlock);
    return entry->deleteGet(key, val);
}

template <class K, class V, class HashFunc>
void SynchronizedHashTable<K, V, HashFunc>::releaseNode(HashNode<K, V> *&node) {
    delete node;
    node = NULL;
}

inline size_t nextBucketNum(size_t n) {
    static const size_t prime_list[] = {
        5ul,        11ul,        17ul,        29ul,        37ul,        53ul,         67ul,         79ul,
        97ul,       131ul,       193ul,       257ul,       389ul,       521ul,        769ul,        1031ul,
        1543ul,     2053ul,      3079ul,      6151ul,      12289ul,     24593ul,      49157ul,      98317ul,
        196613ul,   393241ul,    786433ul,    1572869ul,   3145739ul,   6291469ul,    12582917ul,   25165843ul,
        50331653ul, 100663319ul, 201326611ul, 402653189ul, 805306457ul, 1610612741ul, 3221225473ul, 4294967291ul};

    static const size_t length = sizeof(prime_list) / sizeof(size_t);

    const size_t *bound = std::lower_bound(prime_list, prime_list + length - 1, n);

    return *bound;
}

} // namespace autil
