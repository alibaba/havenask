#include "indexlib/index/kkv/KKVShardRecordIterator.h"

#include "indexlib/base/Types.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "unittest/unittest.h"

using namespace indexlib::index_base;

namespace indexlibv2 { namespace index {

class KKVShardRecordIteratorTest : public TESTBASE
{
private:
    using KKVShardRecordIteratorTyped = KKVShardRecordIterator<int32_t>;

public:
    KKVShardRecordIteratorTest() {}
    ~KKVShardRecordIteratorTest() {}

public:
    void setUp() override
    {
        _tabletSchema =
            table::KKVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/kkv_schema_record_iterator.json");
        ASSERT_TRUE(_tabletSchema->GetLegacySchema() == nullptr);

        std::string jsonStr = R"( {
        "online_index_config": {
            "build_config": {
                "sharding_column_num" : 1,
                "level_num" : 3,
                "building_memory_limit_mb": 64
            }
        }
        } )";
        _tabletOptions = table::KKVTableTestHelper::CreateTableOptions(jsonStr);

        _rawDocs.emplace_back("cmd=add,pkey=1,skey=1,value=abcd,ts=10000000,locator=0:1");
        _rawDocs.emplace_back("cmd=delete,pkey=2,skey=2,ts=20000000,locator=0:2");
        _rawDocs.emplace_back("cmd=add,pkey=3,skey=3,value=efg,ts=30000000,locator=0:3");
        _rawDocs.emplace_back("cmd=add,pkey=3,skey=4,value=efg2,ts=40000000,locator=0:4");
        _rawDocs.emplace_back("cmd=add,pkey=1,skey=2,value=abcd2,ts=50000000,locator=0:5");
        _rawDocs.emplace_back("cmd=add,pkey=4,skey=4,value=hij,ts=60000000,locator=0:6");
        _rawDocs.emplace_back("cmd=add,pkey=3,skey=5,value=efg3,ts=70000000,locator=0:7");
        _rawDocs.emplace_back("cmd=add,pkey=1,skey=3,value=abcd3,ts=80000000,locator=0:8");
        _rawDocs.emplace_back("cmd=add,pkey=4,skey=5,value=hij2,ts=90000000,locator=0:9");
        _rawDocs.emplace_back("cmd=add,pkey=1,skey=3,value=abcd4,ts=100000000,locator=0:10");
        _rawDocs.emplace_back("cmd=delete,pkey=3,skey=3,ts=110000000,locator=0:11");
        _rawDocs.emplace_back("cmd=delete,pkey=4,ts=120000000,locator=0:12");
        setenv("DISABLE_CODEGEN", "true", 1);
    }
    void tearDown() override { unsetenv("DISABLE_CODEGEN"); }

private:
    void PrepareIterator(KKVShardRecordIteratorTyped& iterator, table::KKVTableTestHelper& helper,
                         const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema,
                         const std::vector<std::string>& rawDocs, uint32_t maxDocCountPerSegment,
                         int64_t currentTs = 0);
    void CheckRecords(KKVShardRecordIteratorTyped& iterator, const std::vector<uint32_t>& expectedKeys);
    void CheckNextAndSeek(KKVShardRecordIteratorTyped& iterator);

private:
    std::vector<std::string> _rawDocs;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _tabletSchema;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KKVShardRecordIteratorTest);

void KKVShardRecordIteratorTest::PrepareIterator(KKVShardRecordIteratorTyped& iterator,
                                                 table::KKVTableTestHelper& helper,
                                                 const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema,
                                                 const std::vector<std::string>& rawDocs,
                                                 uint32_t maxDocCountPerSegment, int64_t currentTs)
{
    std::string subPath = autil::StringUtil::toString(maxDocCountPerSegment);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH() + "/" + subPath, GET_TEMP_DATA_PATH() + "/" + subPath);
    ASSERT_TRUE(helper.Open(indexRoot, tabletSchema, _tabletOptions).IsOK());
    uint32_t docCount = 0;
    ASSERT_TRUE(!rawDocs.empty());
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
    ASSERT_TRUE(iterator.Init(segments, params, tabletSchema, nullptr, currentTs).IsOK());
}

void KKVShardRecordIteratorTest::CheckRecords(KKVShardRecordIteratorTyped& iterator,
                                              const std::vector<uint32_t>& expectedKeys)
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

void KKVShardRecordIteratorTest::CheckNextAndSeek(KKVShardRecordIteratorTyped& iterator)
{
    // test next
    IShardRecordIterator::ShardRecord record;
    IShardRecordIterator::ShardCheckpoint defaultShardCheckpoint;
    std::string checkpoint0((char*)&defaultShardCheckpoint, sizeof(defaultShardCheckpoint));
    std::string checkpoint1;
    ASSERT_TRUE(iterator.HasNext());
    ASSERT_TRUE(iterator.Next(&record, &checkpoint1).IsOK());
    ASSERT_EQ(uint32_t(1), record.key);
    ASSERT_EQ(uint32_t(10), record.timestamp);

    ASSERT_TRUE(iterator.HasNext());
    std::string checkpoint2;
    ASSERT_TRUE(iterator.Next(&record, &checkpoint2).IsOK());
    ASSERT_EQ(uint32_t(1), record.key);
    ASSERT_EQ(uint32_t(50), record.timestamp);

    ASSERT_TRUE(iterator.HasNext());
    std::string checkpoint3;
    ASSERT_TRUE(iterator.Next(&record, &checkpoint3).IsOK());
    ASSERT_EQ(uint32_t(1), record.key);
    ASSERT_EQ(uint32_t(100), record.timestamp);

    ASSERT_TRUE(iterator.HasNext());
    std::string checkpoint4;
    ASSERT_TRUE(iterator.Next(&record, &checkpoint4).IsOK());
    ASSERT_EQ(uint32_t(3), record.key);
    ASSERT_EQ(uint32_t(40), record.timestamp);

    ASSERT_TRUE(iterator.HasNext());
    std::string checkpoint5;
    ASSERT_TRUE(iterator.Next(&record, &checkpoint5).IsOK());
    ASSERT_EQ(uint32_t(3), record.key);
    ASSERT_EQ(uint32_t(70), record.timestamp);

    std::string checkpoint6 = checkpoint5;
    IShardRecordIterator::ShardCheckpoint* shardCheckpoint6 =
        (IShardRecordIterator::ShardCheckpoint*)checkpoint6.data();
    shardCheckpoint6->first++;

    ASSERT_FALSE(iterator.HasNext());

    // test seek
    auto status = iterator.Seek(checkpoint2);
    std::vector<uint32_t> expectedKeys = {1, 1, 3, 3};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint1);
    expectedKeys = {1, 1, 1, 3, 3};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint6);
    expectedKeys = {};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint0);
    expectedKeys = {1, 1, 1, 3, 3};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint3);
    expectedKeys = {1, 3, 3};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint4);
    expectedKeys = {3, 3};
    CheckRecords(iterator, expectedKeys);

    status = iterator.Seek(checkpoint5);
    expectedKeys = {3};
    CheckRecords(iterator, expectedKeys);
}

TEST_F(KKVShardRecordIteratorTest, TestNextAndSeek)
{
    {
        KKVShardRecordIteratorTyped iterator;
        table::KKVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(1));
        CheckNextAndSeek(iterator);
    }
    {
        KKVShardRecordIteratorTyped iterator;
        table::KKVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(2));
        CheckNextAndSeek(iterator);
    }
    {
        KKVShardRecordIteratorTyped iterator;
        table::KKVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(3));
        CheckNextAndSeek(iterator);
    }
    {
        KKVShardRecordIteratorTyped iterator;
        table::KKVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(4));
        CheckNextAndSeek(iterator);
    }
    {
        KKVShardRecordIteratorTyped iterator;
        table::KKVTableTestHelper helper;
        PrepareIterator(iterator, helper, _tabletSchema, _rawDocs, uint32_t(5));
        CheckNextAndSeek(iterator);
    }
}

TEST_F(KKVShardRecordIteratorTest, TestPKRawValue)
{
    auto tabletSchema =
        table::KKVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/kkv_schema_for_pk_value.json");
    ASSERT_TRUE(tabletSchema);
    KKVShardRecordIterator<int32_t> iterator;
    table::KKVTableTestHelper helper;
    PrepareIterator(iterator, helper, tabletSchema, _rawDocs, uint32_t(1), 10000000);

    IShardRecordIterator::ShardRecord record;
    std::string checkpoint;
    for (int32_t i = 0; i < 5; ++i) {
        ASSERT_TRUE(iterator.HasNext());
        ASSERT_TRUE(iterator.Next(&record, &checkpoint).IsOK());
        ASSERT_EQ(2, record.otherFields.size());
        ASSERT_TRUE(record.otherFields.end() != record.otherFields.find("pkey"));
    }
    ASSERT_FALSE(iterator.HasNext());

    const auto& kkvIndexConfig = iterator.TEST_GetKKVIndexConfig();
    const auto& pkValueIndexConfig = iterator.TEST_GetPKValueIndexConfig();
    ASSERT_EQ(pkValueIndexConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct(),
              kkvIndexConfig->GetIndexPreference().GetHashDictParam().GetOccupancyPct());
    ASSERT_EQ(pkValueIndexConfig->GetTTL(), kkvIndexConfig->GetTTL());
}

}} // namespace indexlibv2::index
