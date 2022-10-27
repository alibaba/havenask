/*********************************************************************
 * $Author: santai.ww $
 *
 * $LastChangedBy: santai.ww $
 *
 * $Revision: 9613 $
 *
 * $LastChangedDate: 2012-01-06 22:04:49 +0800 (Fri, 06 Jan 2012) $
 *
 * $Id: CacheMap.h 9613 2012-01-06 14:04:49Z santai.ww $
 ********************************************************************/

#ifndef KINGSO_CACHE_CACHEMAP_H
#define KINGSO_CACHE_CACHEMAP_H

#include <stdio.h>
#include <atomic>
#include <assert.h>
#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <autil/Lock.h>
#include "CacheItem.h"

BEGIN_HA3_NAMESPACE(util);

#define CACHE_MAX_SIZESTEP 24

extern uint32_t szHashMap_StepTable3[];
typedef CacheItem HashEntry;
typedef HashEntry *HashEntryPtr;

class CCacheMap
{
private:
    //查找hash step表,决定hash size应该是多大比较合适
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
    std::atomic_ulong m_nHashUsed;
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
    /* printf hash size*/
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

    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(util);

#endif
