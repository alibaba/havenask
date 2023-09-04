#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentIterator.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentIteratorOption.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace autil::legacy::json;
using namespace autil::legacy;

namespace indexlibv2::index {

class KKVBuiltSegmentValidateSKeyTest : public TESTBASE
{
    using PooledSKeySet = typename SKeySearchContext<int64_t>::PooledSKeySet;

protected:
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> BuildIndexConfig(bool storeExpireTime);

private:
    autil::mem_pool::Pool _pool;
};

TEST_F(KKVBuiltSegmentValidateSKeyTest, testTimestamp)
{
    static constexpr bool storeExpireTime = false;
    auto indexConfig = BuildIndexConfig(storeExpireTime);
    bool keepSortSeq = true;
    uint32_t ts = 1024u;
    uint32_t curTsInSecond = 2048u;

    using Option = KKVBuiltSegmentIteratorOption<false, false, storeExpireTime>;
    using SegmentIterator = KKVBuiltSegmentIterator<int64_t, Option>;
    SegmentIterator iterator(indexConfig.get(), keepSortSeq, ts, true, &_pool, nullptr);

    iterator._skey = 0;
    iterator._skeyDeleted = false;
    iterator._timestamp = ts;
    iterator._expireTime = 0;

    PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
    {
        uint32_t minTsInSecond = ts + 1;
        // _timestamp < minTsInSecond
        ASSERT_EQ(SegmentIterator::SKeyStatus::FILTERED,
                  iterator.ValidateSKey(nullptr, minTsInSecond, curTsInSecond, foundSKeys));
        ASSERT_TRUE(foundSKeys.empty());
    }
    {
        // _timestamp > minTsInSecond
        uint32_t minTsInSecond = ts - 1;
        ASSERT_EQ(SegmentIterator::SKeyStatus::VALID,
                  iterator.ValidateSKey(nullptr, minTsInSecond, curTsInSecond, foundSKeys));
        ASSERT_EQ(1, foundSKeys.size());
    }
}

TEST_F(KKVBuiltSegmentValidateSKeyTest, testSKeyContext)
{
    static constexpr bool storeExpireTime = false;
    auto indexConfig = BuildIndexConfig(storeExpireTime);
    bool keepSortSeq = false;
    uint32_t ts = 1024u;
    uint32_t curTsInSecond = 2048u;

    using Option = KKVBuiltSegmentIteratorOption<false, false, storeExpireTime>;
    using SegmentIterator = KKVBuiltSegmentIterator<int64_t, Option>;
    SegmentIterator iterator(indexConfig.get(), keepSortSeq, ts, true, &_pool, nullptr);

    iterator._skey = 0;
    iterator._skeyDeleted = false;
    iterator._timestamp = ts;
    iterator._expireTime = 0;

    {
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        SKeySearchContext<int64_t> skeyContext(&_pool);
        skeyContext.Init(vector<uint64_t> {1});
        uint32_t minTsInSecond = ts - 1;
        // _skey != 1
        ASSERT_EQ(SegmentIterator::SKeyStatus::FILTERED,
                  iterator.ValidateSKey(&skeyContext, minTsInSecond, curTsInSecond, foundSKeys));
        ASSERT_TRUE(foundSKeys.empty());
    }
    {
        // _skey == 0
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        SKeySearchContext<int64_t> skeyContext(&_pool);
        skeyContext.Init(vector<uint64_t> {0});
        uint32_t minTsInSecond = ts - 1;
        ASSERT_EQ(SegmentIterator::SKeyStatus::VALID,
                  iterator.ValidateSKey(&skeyContext, minTsInSecond, curTsInSecond, foundSKeys));
        ASSERT_EQ(1, foundSKeys.size());
    }
    {
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        foundSKeys.insert(0);
        SKeySearchContext<int64_t> skeyContext(&_pool);
        skeyContext.Init(vector<uint64_t> {0});
        uint32_t minTsInSecond = ts - 1;
        //  foundSKeys.insert(0);
        ASSERT_EQ(SegmentIterator::SKeyStatus::INVALID,
                  iterator.ValidateSKey(&skeyContext, minTsInSecond, curTsInSecond, foundSKeys));
        ASSERT_EQ(1, foundSKeys.size());
    }
    {
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        SKeySearchContext<int64_t> skeyContext(&_pool);
        skeyContext.Init(vector<uint64_t> {(uint64_t)-1});
        // 0 > -1
        uint32_t minTsInSecond = ts - 1;
        ASSERT_EQ(SegmentIterator::SKeyStatus::INVALID,
                  iterator.ValidateSKey(&skeyContext, minTsInSecond, curTsInSecond, foundSKeys));
        ASSERT_EQ(0, foundSKeys.size());
    }
}

TEST_F(KKVBuiltSegmentValidateSKeyTest, testKeepSortSeq)
{
    static constexpr bool storeExpireTime = false;
    auto indexConfig = BuildIndexConfig(storeExpireTime);
    bool keepSortSeq = true;
    uint32_t ts = 1024u;
    uint32_t curTsInSecond = 2048u;

    using Option = KKVBuiltSegmentIteratorOption<false, false, storeExpireTime>;
    using SegmentIterator = KKVBuiltSegmentIterator<int64_t, Option>;
    SegmentIterator iterator(indexConfig.get(), keepSortSeq, ts, true, &_pool, nullptr);

    iterator._skey = 0;
    iterator._skeyDeleted = false;
    iterator._timestamp = ts;
    iterator._expireTime = 0;

    PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
    SKeySearchContext<int64_t> skeyContext(&_pool);
    skeyContext.Init(vector<uint64_t> {(uint64_t)-1});
    uint32_t minTsInSecond = ts - 1;
    ASSERT_EQ(SegmentIterator::SKeyStatus::FILTERED,
              iterator.ValidateSKey(&skeyContext, minTsInSecond, curTsInSecond, foundSKeys));
    ASSERT_EQ(0, foundSKeys.size());
}

TEST_F(KKVBuiltSegmentValidateSKeyTest, testStoreExpireTime)
{
    static constexpr bool storeExpireTime = true;
    auto indexConfig = BuildIndexConfig(storeExpireTime);
    bool keepSortSeq = true;
    uint32_t ts = 1024u;
    uint32_t curTsInSecond = 2048u;

    using Option = KKVBuiltSegmentIteratorOption<false, false, storeExpireTime>;
    using SegmentIterator = KKVBuiltSegmentIterator<int64_t, Option>;
    SegmentIterator iterator(indexConfig.get(), keepSortSeq, ts, true, &_pool, nullptr);

    iterator._skey = 0;
    iterator._skeyDeleted = false;
    iterator._timestamp = ts;
    {
        // _expireTime < curTsInSecond
        iterator._expireTime = curTsInSecond - 1;
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        ASSERT_EQ(SegmentIterator::SKeyStatus::FILTERED, iterator.ValidateSKey(nullptr, 0, curTsInSecond, foundSKeys));
        ASSERT_EQ(1, foundSKeys.size());
    }
    {
        // _expireTime > curTsInSecond
        iterator._expireTime = curTsInSecond + 1;
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        ASSERT_EQ(SegmentIterator::SKeyStatus::VALID, iterator.ValidateSKey(nullptr, 0, curTsInSecond, foundSKeys));
        ASSERT_EQ(1, foundSKeys.size());
    }
}

TEST_F(KKVBuiltSegmentValidateSKeyTest, testStoreExpireTimeButValueIsZero)
{
    static constexpr bool storeExpireTime = true;
    auto indexConfig = BuildIndexConfig(storeExpireTime);

    bool keepSortSeq = true;
    uint32_t ts = 1024u;
    uint32_t curTsInSecond = 2048u;

    using Option = KKVBuiltSegmentIteratorOption<false, false, storeExpireTime>;
    using SegmentIterator = KKVBuiltSegmentIterator<int64_t, Option>;
    {
        uint64_t ttl = 1024;
        indexConfig->SetTTL(ttl);
        SegmentIterator iterator(indexConfig.get(), keepSortSeq, ts, true, &_pool, nullptr);

        iterator._skey = 0;
        iterator._skeyDeleted = false;
        iterator._timestamp = ts;
        iterator._expireTime = 0;

        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        // ttl + _timestamp == curTsInSecond
        ASSERT_EQ(SegmentIterator::SKeyStatus::VALID, iterator.ValidateSKey(nullptr, 0, curTsInSecond, foundSKeys));
        ASSERT_EQ(1, foundSKeys.size());
    }
    {
        uint64_t ttl = 1023;
        indexConfig->SetTTL(ttl);
        SegmentIterator iterator(indexConfig.get(), keepSortSeq, ts, true, &_pool, nullptr);

        iterator._skey = 0;
        iterator._skeyDeleted = false;
        iterator._timestamp = ts;
        iterator._expireTime = 0;
        // ttl + _timestamp > curTsInSecond
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        SKeySearchContext<int64_t> skeyContext(&_pool);
        skeyContext.Init(vector<uint64_t> {(uint64_t)-1});
        ASSERT_EQ(SegmentIterator::SKeyStatus::FILTERED, iterator.ValidateSKey(nullptr, 0, curTsInSecond, foundSKeys));
        ASSERT_EQ(1, foundSKeys.size());
    }
}

TEST_F(KKVBuiltSegmentValidateSKeyTest, testFoundSKeys)
{
    static constexpr bool storeExpireTime = false;
    auto indexConfig = BuildIndexConfig(storeExpireTime);
    bool keepSortSeq = true;
    uint32_t ts = 1024u;
    uint32_t curTsInSecond = 2048u;

    using Option = KKVBuiltSegmentIteratorOption<false, false, storeExpireTime>;
    using SegmentIterator = KKVBuiltSegmentIterator<int64_t, Option>;
    SegmentIterator iterator(indexConfig.get(), keepSortSeq, ts, true, &_pool, nullptr);

    iterator._skey = 0;
    iterator._skeyDeleted = false;
    iterator._timestamp = ts;
    iterator._expireTime = 0;
    {
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        ASSERT_EQ(SegmentIterator::SKeyStatus::VALID, iterator.ValidateSKey(nullptr, 0, curTsInSecond, foundSKeys));
        ASSERT_EQ(1, foundSKeys.size());
    }
    {
        iterator.SetIsOnlySegment(true);
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        ASSERT_EQ(SegmentIterator::SKeyStatus::VALID, iterator.ValidateSKey(nullptr, 0, curTsInSecond, foundSKeys));
        ASSERT_EQ(0, foundSKeys.size());
    }
    {
        iterator.SetIsOnlySegment(false);
        iterator.SetIsLastSegment(true);
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        ASSERT_EQ(SegmentIterator::SKeyStatus::VALID, iterator.ValidateSKey(nullptr, 0, curTsInSecond, foundSKeys));
        ASSERT_EQ(0, foundSKeys.size());
    }
    {
        PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
        SKeySearchContext<int64_t> skeyContext(&_pool);
        skeyContext.Init(vector<uint64_t> {0});
        ASSERT_EQ(SegmentIterator::SKeyStatus::VALID,
                  iterator.ValidateSKey(&skeyContext, 0, curTsInSecond, foundSKeys));
        ASSERT_EQ(1, foundSKeys.size());
    }
}
TEST_F(KKVBuiltSegmentValidateSKeyTest, testDeletedSKeys)
{
    static constexpr bool storeExpireTime = false;
    auto indexConfig = BuildIndexConfig(storeExpireTime);
    bool keepSortSeq = true;
    uint32_t ts = 1024u;
    uint32_t curTsInSecond = 2048u;

    using Option = KKVBuiltSegmentIteratorOption<false, false, storeExpireTime>;
    using SegmentIterator = KKVBuiltSegmentIterator<int64_t, Option>;
    SegmentIterator iterator(indexConfig.get(), keepSortSeq, ts, true, &_pool, nullptr);

    iterator._skey = 0;
    iterator._skeyDeleted = true;
    iterator._timestamp = ts;
    iterator._expireTime = 0;

    PooledSKeySet foundSKeys {pool_allocator<int64_t>(&_pool)};
    ASSERT_EQ(SegmentIterator::SKeyStatus::FILTERED, iterator.ValidateSKey(nullptr, 0, curTsInSecond, foundSKeys));
    ASSERT_EQ(1, foundSKeys.size());
}

std::shared_ptr<indexlibv2::config::KKVIndexConfig>
KKVBuiltSegmentValidateSKeyTest::BuildIndexConfig(bool storeExpireTime)
{
    string indexConfigStr = R"({
            "index_name": "pkey_skey",
            "index_type": "PRIMARY_KEY",
            "index_fields": [
                {"field_name": "uid","key_type": "prefix"},
                {"field_name": "friendid","key_type": "suffix"}
            ],
            "value_fields": ["value"],
            "store_expire_time": false
          })";
    JsonMap jsonMap;
    FromJsonString(jsonMap, indexConfigStr);

    if (storeExpireTime) {
        jsonMap["store_expire_time"] = true;
    }
    auto any = ToJson(jsonMap);
    auto fieldConfigs = config::FieldConfig::TEST_CreateFields("uid:long;friendid:long;value:string");
    config::MutableJson runtimeSettings;
    config::IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);

    auto indexConfig = std::make_shared<indexlibv2::config::KKVIndexConfig>();
    indexConfig->Deserialize(any, 0, resource);
    return indexConfig;
}

} // namespace indexlibv2::index
