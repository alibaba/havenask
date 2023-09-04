#include "indexlib/index/kv/KVShardRecordIterator.h"

#include "autil/HashAlgorithm.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "unittest/unittest.h"

using namespace indexlib::index_base;

namespace indexlibv2 { namespace index {

class KVShardRecordIteratorTest : public TESTBASE
{
public:
    KVShardRecordIteratorTest() {}
    ~KVShardRecordIteratorTest() {}

public:
    void setUp() override
    {
        int64_t ttl = 3;
        std::string field = "key:int32;value:string";
        _tabletSchema = indexlibv2::table::KVTabletSchemaMaker::Make(field, "key", "value", ttl);
        auto indexConfigs = _tabletSchema->GetIndexConfigs();
        if (!indexConfigs.empty()) {
            auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfigs[0]);
            kvConfig->SetTTL(ttl);
        }

        _tabletOptions = std::make_shared<config::TabletOptions>();
        std::string jsonStr = R"( {
        "online_index_config": {
            "build_config": {
                "sharding_column_num" : 1,
                "level_num" : 3
            }
        }
        } )";
        FromJsonString(*_tabletOptions, jsonStr);
        _tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);

        _rawDocs.emplace_back("cmd=add,key=1,value=abcd,ts=10000000,locator=0:1");
        _rawDocs.emplace_back("cmd=delete,key=2,ts=30000000,locator=0:2");
        _rawDocs.emplace_back("cmd=add,key=3,value=efg,ts=40000000,locator=0:3");
        _rawDocs.emplace_back("cmd=add,key=4,value=hij,ts=50000000,locator=0:4");
        setenv("DISABLE_CODEGEN", "true", 1);
    }
    void tearDown() override { unsetenv("DISABLE_CODEGEN"); }

private:
    void PrepareIterator(KVShardRecordIterator& iterator, table::KVTableTestHelper& helper,
                         const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema,
                         const std::vector<std::string>& rawDocs, uint32_t maxDocCountPerSegment,
                         int64_t currentTs = 0);
    void CheckRecords(KVShardRecordIterator& iterator, const std::vector<keytype_t>& expectedKeys);
    void CheckNextAndSeek(KVShardRecordIterator& iterator);

private:
    std::vector<std::string> _rawDocs;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _tabletSchema;
    std::shared_ptr<index::AdapterIgnoreFieldCalculator> _ignoreFieldCalculator;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KVShardRecordIteratorTest);

void KVShardRecordIteratorTest::PrepareIterator(KVShardRecordIterator& iterator, table::KVTableTestHelper& helper,
                                                const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema,
                                                const std::vector<std::string>& rawDocs, uint32_t maxDocCountPerSegment,
                                                int64_t currentTs)
{
    std::string subPath = autil::StringUtil::toString(maxDocCountPerSegment);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH() + "/" + subPath, GET_TEMP_DATA_PATH() + "/" + subPath);
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, _tabletOptions).IsOK());
    uint32_t docCount = 0;
    assert(!rawDocs.empty());
    for (size_t i = 0; i < rawDocs.size() - 1; ++i) {
        if (++docCount % maxDocCountPerSegment == 0) {
            ASSERT_TRUE(helper.BuildSegment(rawDocs[i], true).IsOK());
        } else {
            ASSERT_TRUE(helper.Build(rawDocs[i], true).IsOK());
        }
    }
    ASSERT_TRUE(helper.BuildSegment(rawDocs[rawDocs.size() - 1]).IsOK());
    std::vector<std::shared_ptr<indexlibv2::framework::Segment>> segments;
    const auto& multiShardSegments = helper.GetTablet()->GetTabletData()->TEST_GetSegments();
    for (auto segment : multiShardSegments) {
        auto multiShardSegment = std::dynamic_pointer_cast<indexlibv2::plain::MultiShardDiskSegment>(segment);
        ASSERT_TRUE(multiShardSegment);
        ASSERT_EQ(uint32_t(1), multiShardSegment->GetShardCount());
        segments.emplace_back(multiShardSegment->GetShardSegment(0));
    }
    std::map<std::string, std::string> params;
    _ignoreFieldCalculator = std::make_shared<index::AdapterIgnoreFieldCalculator>();
    const auto& indexConfigs = tabletSchema->GetIndexConfigs(index::KV_INDEX_TYPE_STR);
    ASSERT_TRUE(indexConfigs.size() <= 2);
    for (const auto& indexConfig : indexConfigs) {
        auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        if (KV_RAW_KEY_INDEX_NAME != kvConfig->GetIndexName()) {
            ASSERT_TRUE(_ignoreFieldCalculator->Init(kvConfig, helper.GetTablet()->GetTabletData().get()));
            break;
        }
    }
    std::reverse(segments.begin(), segments.end());
    ASSERT_TRUE(iterator.Init(segments, params, tabletSchema, _ignoreFieldCalculator, currentTs).IsOK());
}

void KVShardRecordIteratorTest::CheckRecords(KVShardRecordIterator& iterator,
                                             const std::vector<keytype_t>& expectedKeys)
{
    IShardRecordIterator::ShardRecord record;
    std::string checkpoint;
    for (auto key : expectedKeys) {
        ASSERT_TRUE(iterator.HasNext());
        ASSERT_TRUE(iterator.Next(&record, &checkpoint).IsOK());
        ASSERT_EQ(key, record.key);
    }
    ASSERT_TRUE(iterator.Next(&record, &checkpoint).IsEof());
    ASSERT_FALSE(iterator.HasNext());
}

void KVShardRecordIteratorTest::CheckNextAndSeek(KVShardRecordIterator& iterator)
{
    // test next
    IShardRecordIterator::ShardRecord record;
    IShardRecordIterator::ShardCheckpoint defaultShardCheckpoint;
    std::string checkpoint0((char*)&defaultShardCheckpoint, sizeof(defaultShardCheckpoint));
    std::string checkpoint1;
    ASSERT_TRUE(iterator.HasNext());
    ASSERT_TRUE(iterator.Next(&record, &checkpoint1).IsOK());
    ASSERT_EQ(keytype_t(1), record.key);

    ASSERT_TRUE(iterator.HasNext());
    std::string checkpoint2;
    ASSERT_TRUE(iterator.Next(&record, &checkpoint2).IsOK());
    ASSERT_EQ(keytype_t(3), record.key);

    ASSERT_TRUE(iterator.HasNext());
    std::string checkpoint3;
    ASSERT_TRUE(iterator.Next(&record, &checkpoint3).IsOK());
    ASSERT_EQ(keytype_t(4), record.key);

    std::string checkpoint4 = checkpoint3;
    IShardRecordIterator::ShardCheckpoint* shardCheckpoint = (IShardRecordIterator::ShardCheckpoint*)checkpoint4.data();
    shardCheckpoint->first++;

    ASSERT_FALSE(iterator.HasNext());

    // test seek
    auto status = iterator.Seek(checkpoint2);
    std::vector<keytype_t> expectedKeys = {3, 4};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint1);
    expectedKeys = {1, 3, 4};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint4);
    expectedKeys = {};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint0);
    expectedKeys = {1, 3, 4};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint3);
    expectedKeys = {4};
    CheckRecords(iterator, expectedKeys);
}

TEST_F(KVShardRecordIteratorTest, TestNextAndSeek)
{
    // 4 segments
    {
        KVShardRecordIterator iterator;
        table::KVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(1));
        CheckNextAndSeek(iterator);
    }
    // 2 segments
    {
        KVShardRecordIterator iterator;
        table::KVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(2));
        CheckNextAndSeek(iterator);
    }
    // 2 segments
    {
        KVShardRecordIterator iterator;
        table::KVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(3));
        CheckNextAndSeek(iterator);
    }
    // 1 segments
    {
        KVShardRecordIterator iterator;
        table::KVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(4));
        CheckNextAndSeek(iterator);
    }
}

TEST_F(KVShardRecordIteratorTest, TestTTL)
{
    {
        KVShardRecordIterator iterator;
        table::KVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(1), 10000000);
        std::vector<keytype_t> expectedKeys = {1, 3, 4};
        CheckRecords(iterator, expectedKeys);
    }
    {
        KVShardRecordIterator iterator;
        table::KVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(1), 40000000);
        std::vector<keytype_t> expectedKeys = {3, 4};
        CheckRecords(iterator, expectedKeys);
    }
    {
        KVShardRecordIterator iterator;
        table::KVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(1), 50000000);
        std::vector<keytype_t> expectedKeys = {4};
        CheckRecords(iterator, expectedKeys);
    }
    {
        KVShardRecordIterator iterator;
        table::KVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(1), 60000000);
        std::vector<keytype_t> expectedKeys = {};
        CheckRecords(iterator, expectedKeys);
    }
}

TEST_F(KVShardRecordIteratorTest, TestPKValue)
{
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(false);
    auto tabletSchema =
        table::KVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "kv_schema_for_pk_value.json");
    ASSERT_TRUE(tabletSchema);
    std::vector<std::string> rawDocs;
    auto indexNameHash1 = autil::StringUtil::toString(autil::HashAlgorithm::hashString64("raw_key"));
    auto indexNameHash2 = autil::StringUtil::toString(autil::HashAlgorithm::hashString64("index2"));
    for (auto doc : _rawDocs) {
        rawDocs.emplace_back(doc + ",region_name=index2,index_name_hash=" + indexNameHash2);
    }
    KVShardRecordIterator iterator;
    table::KVTableTestHelper helper;
    PrepareIterator(iterator, helper, tabletSchema, rawDocs, uint32_t(1), 10000000);

    IShardRecordIterator::ShardRecord record;
    std::string checkpoint;
    ASSERT_TRUE(iterator.HasNext());
    ASSERT_TRUE(iterator.Next(&record, &checkpoint).IsOK());
    ASSERT_EQ(keytype_t(1), record.key);
    ASSERT_EQ(1, record.otherFields.size());
    ASSERT_TRUE(record.otherFields.end() != record.otherFields.find("key"));

    ASSERT_TRUE(iterator.HasNext());
    ASSERT_TRUE(iterator.Next(&record, &checkpoint).IsOK());
    ASSERT_EQ(keytype_t(3), record.key);
    ASSERT_EQ(1, record.otherFields.size());
    ASSERT_TRUE(record.otherFields.end() != record.otherFields.find("key"));

    ASSERT_TRUE(iterator.HasNext());
    ASSERT_TRUE(iterator.Next(&record, &checkpoint).IsOK());
    ASSERT_EQ(keytype_t(4), record.key);
    ASSERT_EQ(1, record.otherFields.size());
    ASSERT_TRUE(record.otherFields.end() != record.otherFields.find("key"));

    ASSERT_FALSE(iterator.HasNext());
}

}} // namespace indexlibv2::index
