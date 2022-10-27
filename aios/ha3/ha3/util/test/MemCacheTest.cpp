#include <unittest/unittest.h>
#include <autil/Thread.h>
#include <ha3/util/memcache/MemCache.h>
#include <fstream>

using namespace std;
using namespace testing;
using namespace autil;

BEGIN_HA3_NAMESPACE(util);

class MemCacheTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    void put();
    void get();
    void del();
private:
    MemCache *_cache;
    ChainedFixedSizePool *_pool;
    vector<uint64_t> _keys;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, MemCacheTest);

const uint64_t threadCount = 8;
const uint64_t loop = 10;

void MemCacheTest::setUp() {
    _cache = new MemCache;
    _pool = new util::ChainedFixedSizePool;
    _pool->init(1024 - sizeof(void*));
    _cache->init(1073741824, 200000, _pool);

    ifstream ifs("/dev/urandom", ios::in | ios::binary);
    if (!ifs.is_open()) {
        HA3_LOG(INFO, "open /dev/urandom failed");
        return;
    }
    _keys.resize(200000);
    for (size_t count = 0; count < 200000; ++count) {
        ifs.read((char *)&_keys[count], sizeof(uint64_t));
    }
    for (uint64_t count = 0; count < 600000; ++count) {
        uint64_t key;
        ifs.read((char *)&key, sizeof(uint64_t));
        ChainedBlockItem item = _pool->createItem((char *)&_keys[0], 3000);
        MCElem mcKey((char*)&key, sizeof(key));
        MCValueElem mcValue(item, 100);
        _cache->put(mcKey, mcValue);
    }
}

void MemCacheTest::tearDown() {
    delete _cache;
    delete _pool;
}

void MemCacheTest::put() {
    for (uint64_t i = 0; i < 4; ++i) {
        for (size_t j = 0; j < _keys.size(); ++j) {
            uint64_t key = _keys[j];
            ChainedBlockItem item = _pool->createItem((char *)&_keys[0], 3000);
            MCElem mcKey((char*)&key, sizeof(key));
            MCValueElem mcValue(item, 100);
            if (_cache->put(mcKey, mcValue) != 0) {
                HA3_LOG(INFO, "put failed");
                _pool->deallocate(item);
            }
        }
        HA3_LOG(DEBUG, "put %lu", i);
    }
    HA3_LOG(INFO, "put finished");
}

void MemCacheTest::get() {
    for (uint64_t i = 0; i < loop; ++i) {
        for (size_t j = 0; j < _keys.size(); ++j) {
            uint64_t key = _keys[j];
            MCElem mcKey((char*)&key, sizeof(key));
            MemCacheItem *item = _cache->get(mcKey);
            if (item) {
                item->decref();
            }
        }
        HA3_LOG(DEBUG, "get %lu", i);
    }
    HA3_LOG(INFO, "get finished");
}

void MemCacheTest::del() {
    for (uint64_t i = 0; i < loop; ++i) {
        for (size_t j = 0; j < _keys.size(); ++j) {
            uint64_t key = _keys[j];
            MCElem mcKey((char*)&key, sizeof(key));
            _cache->del(mcKey);
        }
        HA3_LOG(DEBUG, "del %lu", i);
    }
    HA3_LOG(INFO, "del finished");
}

TEST_F(MemCacheTest, testSimple) {
    if (threadCount < 8) {
        HA3_LOG(WARN, "threadCount less than 8, bugs triggered under high contention may "
                "not be detected. ******YOU HAVE BEEN WARNED!******");
    }
    vector<ThreadPtr> vt;
    for (size_t i = 0; i < threadCount; ++i) {
        ThreadPtr threadPtr = Thread::createThread(std::tr1::bind(&MemCacheTest::put, this));
        vt.push_back(threadPtr);
    }
    for (size_t i = 0; i < threadCount; ++i) {
        ThreadPtr threadPtr = Thread::createThread(std::tr1::bind(&MemCacheTest::get, this));
        vt.push_back(threadPtr);
    }
    for (size_t i = 0; i < threadCount; ++i) {
        ThreadPtr threadPtr = Thread::createThread(std::tr1::bind(&MemCacheTest::del, this));
        vt.push_back(threadPtr);
    }
    for (size_t i = 0; i < vt.size(); ++i) {
        vt[i]->join();
    }
    uint64_t m_nHashUsed = _cache->_cachemap->m_nHashUsed.load();
    uint64_t itemCount = _cache->_MCItemList->_itemCount.load();
    HA3_LOG(INFO, "m_nHashUsed: %lu", m_nHashUsed);
    HA3_LOG(INFO, "itemCount: %lu", itemCount);
    ASSERT_TRUE(_cache->consistencyCheck());
    HA3_LOG(INFO, "run finished");
}

END_HA3_NAMESPACE();
