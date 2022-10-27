#include "CacheMap.h"
#include <math.h>

BEGIN_HA3_NAMESPACE(util);

HA3_LOG_SETUP(util, CCacheMap);

#define hashlimit(n) ((n * 5)/4)

u_n32_t szHashMap_StepTable3[] = {
	53,97,193,389,769,
	1543,3079,6151,12289,24593,
	49157,98317,196613,393241,786433,
	1572869,3145739,6291469,12582917,25165843,
	50331653,100663319,201326611,402653189,805306457
};

/* 构造函数,hash size的大小默认为szHashMap_StepTable3[0]的值 */
CCacheMap::CCacheMap(u_n8_t cHashSizeStep)
{
 	m_nHashSizeStep = cHashSizeStep > CACHE_MAX_SIZESTEP ? CACHE_MAX_SIZESTEP : cHashSizeStep;
  	m_nHashSize = szHashMap_StepTable3[m_nHashSizeStep];
	m_nLockSize = m_nHashSize / 100;
	if (m_nLockSize == 0) m_nLockSize = m_nHashSize;
	m_nRehashLimit = hashlimit(m_nHashSize);
	m_nHashUsed = 0;
	m_ppEntry = NULL;
        m_pBucketLock = NULL;
        m_pNodeLock = NULL;
	m_nUsedSize = 0;
}

/* 构造函数, 直接指定需要多大的空间来存放元素 */
CCacheMap::CCacheMap(u_n32_t nHashSize)
{
  	m_nHashSizeStep = getHashSizeStep(nHashSize);
	m_nHashSize = nHashSize;
	m_nLockSize = m_nHashSize / 100;
	if (m_nLockSize == 0) m_nLockSize = m_nHashSize;
	m_nRehashLimit = hashlimit(m_nHashSize);
	m_nHashUsed = 0;
	m_ppEntry = NULL;
    m_pBucketLock = NULL;
    m_pNodeLock = NULL;
	m_nUsedSize = 0;
}

/* 析构函数 */
CCacheMap::~CCacheMap(void)
{
    clear();				//清空所有节点的内存
    if(m_ppEntry)
    {
        free(m_ppEntry);
        m_ppEntry = NULL;
    }
    if(m_pBucketLock)
    {
        delete m_pBucketLock;
        m_pBucketLock = NULL;
    }
    if(m_pNodeLock)
    {
        delete[] m_pNodeLock;
        m_pNodeLock = NULL;
    }
	m_nUsedSize = 0;
}

u_n32_t CCacheMap::init(void)
{
  	u_n32_t nSize = m_nHashSize * sizeof(HashEntryPtr), nPrevUsedSize = m_nUsedSize;
    m_ppEntry = (HashEntryPtr*)malloc(nSize);
    if(m_ppEntry == NULL)
    {
        HA3_LOG(ERROR, "CCacheMap: malloc failed!");
        return 0;
    }
    memset(m_ppEntry, 0x0, nSize);
	m_nUsedSize += nSize;
    m_pBucketLock = new autil::ReadWriteLock();
    if(m_pBucketLock == NULL)
    {
        HA3_LOG(ERROR, "CCacheMap: new failed!");
        return 0;
    }
	m_nUsedSize += sizeof(autil::ReadWriteLock);
    m_pNodeLock = new autil::ReadWriteLock[m_nLockSize];
    if(m_pNodeLock == NULL)
    {
        HA3_LOG(ERROR, "CCacheMap: new failed!");
        return 0;
    }
	m_nUsedSize += sizeof(autil::ReadWriteLock) * (m_nLockSize);
    return (m_nUsedSize - nPrevUsedSize);
}

int CCacheMap::lockRead(const u_n64_t& _key)
{
    int ret = m_pBucketLock->rdlock();
    if(ret != 0)
        return ret;
    ret = m_pNodeLock[getLockPos(_key)].rdlock();
    if(ret != 0)
        m_pBucketLock->unlock();
    return ret;
}

int CCacheMap::lockWrite(const u_n64_t& _key)
{
    int ret = m_pBucketLock->rdlock();
    if(ret != 0)
        return ret;
    ret = m_pNodeLock[getLockPos(_key)].wrlock();
    if(ret != 0)
        m_pBucketLock->unlock();
    return ret;
}

int CCacheMap::tryLockRead(const u_n64_t& _key)
{
    int ret = m_pBucketLock->rdlock();
    if(ret != 0)
        return ret;
    ret = m_pNodeLock[getLockPos(_key)].tryrdlock();
    if(ret != 0)
        m_pBucketLock->unlock();
    return ret;
}

int CCacheMap::tryLockWrite(const u_n64_t& _key)
{
    int ret = m_pBucketLock->rdlock();
    if(ret != 0)
        return ret;
    ret = m_pNodeLock[getLockPos(_key)].trywrlock();
    if(ret != 0)
        m_pBucketLock->unlock();
    return ret;
}

int CCacheMap::unlock(const u_n64_t& _key)
{
    int ret1 = m_pNodeLock[getLockPos(_key)].unlock();
    int ret2 = m_pBucketLock->unlock();
    if(ret1 != 0)
		return ret1;
    else
	  	return ret2;
}

int CCacheMap::lockBucket()
{
    return m_pBucketLock->wrlock();
}

int CCacheMap::unlockBucket()
{
    return m_pBucketLock->unlock();
}

/* print */
void CCacheMap::print()
{
	for (u_n32_t i=0; i<m_nHashSize; i++) {
		n32_t nSize = 0;
		HashEntry *pEntry = m_ppEntry[i];
		while (pEntry != NULL) {
			nSize++;
			pEntry = pEntry->hash_next;
		}
		printf("hash i=%d, nSize=%d.\n", i, nSize);
	}
	printf("Hash map size=%d.\n", this->size());
}

/* 向容器中插入key和value */
// 不考虑是否已经存在，在外围已经做过判断
bool CCacheMap::insert(HashEntry *item)
{
	u_n32_t nPos = item->key % m_nHashSize;			//计算key的hash值
        // HA3_LOG(INFO, "insert %p, key %u, nPos %u", item, item->key, nPos);
    item->hash_next = m_ppEntry[nPos];
	m_ppEntry[nPos] = item;
	++m_nHashUsed;
    return true;
}

/* 容器里元素的大小超过总容量的3/5, 需要重新构造该hashmap, 返回增加的内存size */
u_n32_t CCacheMap::rehash()
{
    if (m_nHashUsed.load() < m_nRehashLimit || m_nHashSizeStep >= CACHE_MAX_SIZESTEP)
        return 0;
	if(lockBucket() != 0)
		return 0;
    /* 判断容器是否超过限制需重新rehash */
	if (m_nHashUsed.load() >= m_nRehashLimit && m_nHashSizeStep < CACHE_MAX_SIZESTEP)
        m_nHashSizeStep++;
    else {
		unlockBucket();
        return 0;
	}
    autil::ReadWriteLock *pNodeLockTmp = m_pNodeLock;
	u_n32_t nOldLockSize = m_nLockSize;
	u_n32_t nHashSize = szHashMap_StepTable3[m_nHashSizeStep];
	m_nLockSize = nHashSize / 100;
	if (m_nLockSize == 0) m_nLockSize = nHashSize;
    m_pNodeLock = new autil::ReadWriteLock[m_nLockSize];
    if(m_pNodeLock == NULL)
    {
        m_nHashSizeStep--;
        m_pNodeLock = pNodeLockTmp;
		m_nLockSize = nOldLockSize;
        unlockBucket();
        HA3_LOG(ERROR, "HashMap rehash new mem failed!");
        return 0;
    }
    HashEntryPtr *ppNewEntries = (HashEntryPtr*) calloc(nHashSize, sizeof(HashEntryPtr));
    if(!ppNewEntries)
    {
        m_nHashSizeStep--;
        delete[] m_pNodeLock;
        m_pNodeLock = pNodeLockTmp;
		m_nLockSize = nOldLockSize;
        unlockBucket();
        HA3_LOG(ERROR, "HashMap rehash calloc mem failed!");
        return 0;
    }
	u_n32_t nOldHashSize = m_nHashSize;
	m_nHashSize = nHashSize;
	m_nRehashLimit = hashlimit(m_nHashSize);
	u_n32_t nHashKey;
    for(uint32_t i = 0; i < nOldHashSize; i++)
    {
        HashEntry *pEntry = m_ppEntry[i], *pEntry2 = NULL;
        while(pEntry)
        {
            nHashKey = pEntry->key % m_nHashSize;
			pEntry2 = pEntry;
            pEntry = pEntry->hash_next;
            pEntry2->hash_next = ppNewEntries[nHashKey];
            ppNewEntries[nHashKey] = pEntry2;
        }
    }
    HashEntryPtr *ppOldEntries = m_ppEntry;
    m_ppEntry = ppNewEntries;
	u_n32_t nSize = m_nUsedSize;
	m_nUsedSize += (m_nHashSize - nOldHashSize) * sizeof(HashEntryPtr);
	m_nUsedSize += (m_nLockSize - nOldLockSize) * sizeof(autil::ReadWriteLock);
	nSize = m_nUsedSize - nSize;
	unlockBucket();
    delete[] pNodeLockTmp;
    free(ppOldEntries);
	return nSize;
}

/* 根据key返回对应的Entry,如果该容器支持multikey,则该方法可以查找相同key的所有元素 */
HashEntryPtr CCacheMap::lookupEntry(const u_n64_t& _key)
{
	HashEntryPtr pEntry = m_ppEntry[_key % m_nHashSize];
	while (pEntry != NULL && _key != pEntry->key)	//跟同hash的每一个元素相比较
		pEntry = pEntry->hash_next;
	return pEntry;
}

/* 根据key清空某一个元素 */
HashEntry *CCacheMap::remove(const u_n64_t& _key, const HashEntry *entry)
{
	u_n32_t nPos = _key % m_nHashSize;
	HashEntry *pPrevEntry = NULL, *pCurEntry = m_ppEntry[nPos];

	while (pCurEntry != NULL && _key != pCurEntry->key) {
		pPrevEntry = pCurEntry;
		pCurEntry = pCurEntry->hash_next;
	}

    if (pCurEntry != NULL && pCurEntry == entry) {	//考虑到可能是多key值的情况
        // HA3_LOG(INFO, "remove %p, key %u, nPos %u", pCurEntry, pCurEntry->key, nPos);
        if (pPrevEntry == NULL) {					// 该entry的第一个元素
            assert(pCurEntry == m_ppEntry[nPos]);
            m_ppEntry[nPos] = pCurEntry->hash_next;
        } else {
            pPrevEntry->hash_next = pCurEntry->hash_next;
        }
        --m_nHashUsed;
    }
    return pCurEntry;
}

/* 清空容器中的所有元素,但容器大小不做改变 */
void CCacheMap::clear(void)
{
    if (m_nHashUsed.load() > 0) {
		memset(m_ppEntry, 0x0, m_nHashSize*sizeof(HashEntry *));
    }
    m_nHashUsed = 0;
}

/* 根据key查找value值,当key在容器中不存在时,返回none默认值 */
HashEntryPtr CCacheMap::find(const u_n64_t& _key, const HashEntryPtr _none)
{
	HashEntryPtr pEntry = lookupEntry(_key);
	if (pEntry != NULL)
		return pEntry;
	else
		return _none;
}

/* 返回容器中元素的个数 */
u_n32_t CCacheMap::size() const
{
    return m_nHashUsed.load();
}

uint32_t CCacheMap::getLockPos(uint64_t key) const {
    return key % m_nHashSize % m_nLockSize;
}

bool CCacheMap::consistencyCheck(std::map<void *, size_t> &hashMap) {
    for (size_t i = 0; i < m_nHashSize; ++i) {
        HashEntry *entry = m_ppEntry[i];
        while (entry) {
            uint64_t bucket = entry->key % m_nHashSize;
            if (entry->key % m_nHashSize != i) {
                HA3_LOG(ERROR, "wrong position, key %lu, bucket expected %zu, bucket actual %lu",
                        entry->key, i, bucket);
                return false;
            }
            auto iter = hashMap.find(entry);
            if (iter != hashMap.end()) {
                HA3_LOG(ERROR, "duplicate entry in hashmap, key %lu, item %p, bucket1: %zu, bucket2: %zu",
                        entry->key, entry, iter->second, i);
                return false;
            }
            hashMap.insert(std::make_pair(entry, i));
            entry = entry->hash_next;
        }
    }
    return (long)hashMap.size() == m_nHashUsed.load();
}

END_HA3_NAMESPACE(util);
