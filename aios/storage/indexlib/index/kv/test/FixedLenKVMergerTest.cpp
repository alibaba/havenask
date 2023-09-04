#include "indexlib/index/kv/FixedLenKVMerger.h"

#include "autil/Log.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

// kv merge integration test (including fixed len and var len test) is in kv_table/index_task/test

namespace indexlibv2 { namespace index {

class FixedLenKVMergerTest : public TESTBASE
{
public:
    FixedLenKVMergerTest() {}
    ~FixedLenKVMergerTest() {}

public:
    void setUp() override {}
    void tearDown() override {}

    void RewriteConfig(std::shared_ptr<config::KVIndexConfig> kvConfig, bool isLegacy, int32_t ttl,
                       bool useCompactValue);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, FixedLenKVMergerTest);

void FixedLenKVMergerTest::RewriteConfig(std::shared_ptr<config::KVIndexConfig> kvConfig, bool isLegacy, int32_t ttl,
                                         bool useCompactValue)
{
    kvConfig->GetValueConfig()->EnableCompactFormat(!isLegacy);
    if (ttl != -1) {
        kvConfig->SetTTL(ttl);
    }
    if (useCompactValue) {
        auto& kvIndexPreference = kvConfig->GetIndexPreference();
        auto valueParam = kvIndexPreference.GetValueParam();
        valueParam.EnableFixLenAutoInline();
        kvIndexPreference.SetValueParam(valueParam);
    }
}

TEST_F(FixedLenKVMergerTest, TestNeedCompactBucket)
{
    std::vector<SegmentStatistics> statVec;
    map<string, any> params = {{DROP_DELETE_KEY, std::string("true")}, {CURRENT_TIME_IN_SECOND, std::string("100")}};
    for (int i = 0; i < 5; i++) {
        SegmentStatistics stat;
        stat.deletedKeyCount = 0;
        statVec.push_back(stat);
    }
    {
        auto [schema, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("key:int64;value:int8;", "key", "value");
        RewriteConfig(indexConfig, true, -1, true);
        FixedLenKVMerger kvMerger;
        auto status = kvMerger.Init(indexConfig, params);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(kvMerger.NeedCompactBucket(statVec));
    }
    {
        auto [schema, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("key:int64;value:int8;", "key", "value");
        RewriteConfig(indexConfig, false, 10, true);
        FixedLenKVMerger kvMerger;
        auto status = kvMerger.Init(indexConfig, params);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(kvMerger.NeedCompactBucket(statVec));
    }
    {
        auto [schema, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("key:int64;value:int8;", "key", "value");
        RewriteConfig(indexConfig, false, -1, false);
        FixedLenKVMerger kvMerger;
        auto status = kvMerger.Init(indexConfig, params);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(kvMerger.NeedCompactBucket(statVec));
    }
    {
        // // value length > 8
        auto [schema, indexConfig] =
            KVIndexConfigBuilder::MakeIndexConfig("key:int64;value:int64;value1:int32", "key", "value;value1");
        RewriteConfig(indexConfig, false, -1, true);
        FixedLenKVMerger kvMerger;
        auto status = kvMerger.Init(indexConfig, params);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(kvMerger.NeedCompactBucket(statVec));
    }
    {
        // value length == -1
        auto [schema, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("key:int64;value:string;", "key", "value");
        RewriteConfig(indexConfig, false, -1, true);
        FixedLenKVMerger kvMerger;
        auto status = kvMerger.Init(indexConfig, params);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(kvMerger.NeedCompactBucket(statVec));
    }
    {
        // key length <= value length
        auto [schema, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("key:int16;value:int32;", "key", "value");
        RewriteConfig(indexConfig, false, -1, true);
        FixedLenKVMerger kvMerger;
        auto status = kvMerger.Init(indexConfig, params);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(kvMerger.NeedCompactBucket(statVec));
    }
    {
        // merge is bottom level
        auto [schema, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("key:int64;value:int32;", "key", "value");
        RewriteConfig(indexConfig, false, -1, true);
        FixedLenKVMerger kvMerger;
        auto status = kvMerger.Init(indexConfig, params);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(kvMerger.NeedCompactBucket(statVec));
    }
    {
        // merge is not bottom level, but delete count sum = 0
        params = {{DROP_DELETE_KEY, std::string("false")}, {CURRENT_TIME_IN_SECOND, std::string("100")}};

        auto [schema, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("key:int64;value:int32;", "key", "value");
        RewriteConfig(indexConfig, false, -1, true);
        FixedLenKVMerger kvMerger;
        auto status = kvMerger.Init(indexConfig, params);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(kvMerger.NeedCompactBucket(statVec));
    }
    {
        // merge is not bottom level, but delete count sum != 0
        params = {{DROP_DELETE_KEY, std::string("false")}, {CURRENT_TIME_IN_SECOND, std::string("100")}};
        statVec[0].deletedKeyCount = 1;

        auto [schema, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("key:int64;value:int32;", "key", "value");
        RewriteConfig(indexConfig, false, -1, true);
        FixedLenKVMerger kvMerger;
        auto status = kvMerger.Init(indexConfig, params);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(kvMerger.NeedCompactBucket(statVec));
        statVec[0].deletedKeyCount = 0;
    }
}

}} // namespace indexlibv2::index
