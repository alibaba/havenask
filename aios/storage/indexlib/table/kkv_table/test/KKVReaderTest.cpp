#include "fslib/fs/ExceptionTrigger.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/KKVReaderFactory.h"
#include "indexlib/table/kkv_table/KKVReaderImpl.h"
#include "indexlib/table/kkv_table/KKVTabletFactory.h"
#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlibv2::index;
using namespace indexlibv2::framework;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlibv2::table {

class KKVReaderTest : public indexlib::INDEXLIB_TESTBASE_WITH_PARAM<std::string>
{
public:
    KKVReaderTest() {};
    ~KKVReaderTest() {};

public:
    void CaseSetUp() override;
    void PrepareSchema(const std::string& path);
    void PrepareTableOptions(const std::string& jsonStr);

    void DoTestLookup();

private:
    void CheckTTLResult();
    void CheckTTLFromDocResult();

private:
    framework::IndexRoot _indexRoot;
    KKVTableTestHelper _testHelper;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::shared_ptr<autil::mem_pool::Pool> _pool;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, KKVReaderTest);

INSTANTIATE_TEST_CASE_P(TestDifferentValueFormat, KKVReaderTest, testing::Values("", "impact", "plain"));

void KKVReaderTest::CaseSetUp()
{
    _pool = std::make_shared<autil::mem_pool::Pool>();

    _indexRoot = framework::IndexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    std::string tableOptionsJsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3,
            "building_memory_limit_mb": 6,
            "max_doc_count": 4
        }
    }
    } )";
    PrepareTableOptions(tableOptionsJsonStr);
    PrepareSchema("kkv_schema.json");
}

void KKVReaderTest::PrepareTableOptions(const std::string& jsonStr)
{
    _tabletOptions = KKVTableTestHelper::CreateTableOptions(jsonStr);
    _tabletOptions->SetIsOnline(true);
}

void KKVReaderTest::PrepareSchema(const std::string& filename)
{
    _schema = KKVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), filename);
    ASSERT_TRUE(_schema->GetLegacySchema() == nullptr);
    _indexConfig = KKVTableTestHelper::GetIndexConfig(_schema, "pkey");
    KKVTableTestHelper::SetIndexConfigValueParam(_indexConfig, GetParam());
}

void KKVReaderTest::CheckTTLResult()
{
    KKVTableTestHelper& helper = _testHelper;
    {
        // ts = 19
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1#19000000", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2#19000000", "skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3#19000000", "skey=3,value=3"));
    }
    {
        // ts = 111
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1#111000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2#111000000", "skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3#111000000", "skey=3,value=3"));
    }
    {
        // ts = 121
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1#121000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2#121000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3#121000000", "skey=3,value=3"));
    }
    {
        // ts = 131
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1#131000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2#131000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3#131000000", ""));
    }
}

void KKVReaderTest::CheckTTLFromDocResult()
{
    KKVTableTestHelper& helper = _testHelper;
    {
        // ts = 19
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1#19000000", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2#19000000", "skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3#19000000", "skey=3,value=3"));
    }
    {
        // ts = 111
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1#111000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2#111000000", "skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3#111000000", "skey=3,value=3"));
    }
    {
        // ts = 121
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1#121000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2#121000000", "skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3#121000000", "skey=3,value=3"));
    }
    {
        // ts = 221
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1#221000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2#221000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3#221000000", "skey=3,value=3"));
    }
    {
        // ts = 321
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1#321000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2#321000000", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3#321000000", ""));
    }
}

void KKVReaderTest::DoTestLookup()
{
    KKVTableTestHelper& helper = _testHelper;
    std::string docs = "cmd=add,pkey=1,skey=1,value=1,ts=1,locator=0:1;"
                       "cmd=add,pkey=2,skey=2,value=2,ts=2,locator=0:2;"
                       "cmd=add,pkey=1,skey=12,value=12,ts=3,locator=0:3;"
                       "cmd=add,pkey=1,skey=13,value=13,ts=4,locator=0:4;"
                       "cmd=add,pkey=3,skey=3,value=3,ts=5,locator=0:5;"
                       "cmd=add,pkey=3,skey=31,value=31,ts=6,locator=0:6;"
                       "cmd=delete,pkey=2,ts=1000,locator=0:7;"
                       "cmd=delete,pkey=3,skey=31,ts=1000,locator=0:8;"
                       "cmd=delete,pkey=4,ts=1000,locator=0:9;";

    auto status = helper.Build(docs);
    ASSERT_TRUE(status.IsOK()) << status.ToString();

    {
        // lookup only use pkey
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,value=1;skey=12,value=12;skey=13,value=13"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "9999", ""));
        // lookup with skeys
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1:1,13", "skey=1,value=1;skey=13,value=13"));
        // lookup with some not exist skeys
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1:1,13,222,999", "skey=1,value=1;skey=13,value=13"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1:1,6,13,222,999", "skey=1,value=1;skey=13,value=13"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1:999", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1:13", "skey=13,value=13"));
    }

    {
        // lookup deleted pkey
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", ""));
        // lookup deleted not exist pkey
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", ""));
        // lookup deleted skey
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=3,value=3"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3:31", ""));
    }

    {
        // test lookup add after delete
        ASSERT_TRUE(helper.Build("cmd=add,pkey=4,skey=4,value=4,locator=0:10;").IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=4"));
        ASSERT_TRUE(helper.Build("cmd=add,pkey=4,skey=41,value=41,locator=0:11;").IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=4;skey=41,value=41"));
        // delete again
        ASSERT_TRUE(helper.Build("cmd=delete,pkey=4,skey=41,ts=1000,locator=0:12;").IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=4"));

        ASSERT_TRUE(helper.Build("cmd=delete,pkey=4,ts=1000,locator=0:13;").IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", ""));
    }
}

TEST_P(KKVReaderTest, TestCreateKKVReader)
{
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    std::string docs = "cmd=add,pkey=1,skey=1,value=1,ts=1,locator=0:1;"
                       "cmd=add,pkey=2,skey=2,value=2,ts=2,locator=0:2;"
                       "cmd=add,pkey=1,skey=12,value=12,ts=3,locator=0:3;";

    auto status = helper.Build(docs);
    ASSERT_TRUE(status.IsOK()) << status.ToString();

    ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,value=1;skey=12,value=12"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "999", ""));
}

TEST_P(KKVReaderTest, TestLookup)
{
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    {
        DoTestLookup();
        ASSERT_TRUE(helper.BuildSegment("cmd=delete,pkey=-1,ts=-1,locator=0:14;").IsOK());
        ASSERT_GT(helper.GetCurrentVersion().GetSegmentCount(), 3);
    }
}

TEST_P(KKVReaderTest, TestLookupOnlyRt)
{
    std::string tableOptionsJsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3,
            "building_memory_limit_mb": 6,
            "max_doc_count": 100
        }
    }
    } )";
    PrepareTableOptions(tableOptionsJsonStr);
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    DoTestLookup();
    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 0);
}

TEST_P(KKVReaderTest, TestLookupWithMultiValue)
{
    PrepareSchema("kkv_schema_multi_value.json");

    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    string docs = "cmd=add,pkey=1,skey=1,floatmv=1.1 1.2 1.3,ts=1;"
                  "cmd=add,pkey=2,skey=2,floatmv=2.1 2.2 2.3,ts=2;"
                  "cmd=add,pkey=2,skey=21,floatmv=21.1 21.2 21.3,ts=2;";

    {
        ASSERT_TRUE(helper.Build(docs).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,floatmv=1.1 1.2 1.3"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,floatmv=2.1 2.2 2.3;skey=21,floatmv=21.1 21.2 21.3"));
    }

    {
        ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,floatmv=1.1 1.2 1.3"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,floatmv=2.1 2.2 2.3;skey=21,floatmv=21.1 21.2 21.3"));
    }
}

TEST_P(KKVReaderTest, TestLookupWithPackFieldsValue)
{
    PrepareSchema("kkv_schema_pack_value.json");

    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    string docs = "cmd=add,pkey=1,skey=1,intv=1,floatmv=1.1 1.2 1.3,ts=1;"
                  "cmd=add,pkey=2,skey=2,intv=2,floatmv=2.1 2.2 2.3,ts=2;"
                  "cmd=add,pkey=2,skey=21,intv=21,floatmv=21.1 21.2 21.3,ts=2;";

    {
        ASSERT_TRUE(helper.Build(docs).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,floatmv=1.1 1.2 1.3,intv=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2",
                                 "skey=2,floatmv=2.1 2.2 2.3,intv=2;skey=21,floatmv=21.1 21.2 21.3,intv=21"));
    }

    {
        ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,floatmv=1.1 1.2 1.3,intv=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2",
                                 "skey=2,floatmv=2.1 2.2 2.3,intv=2;skey=21,floatmv=21.1 21.2 21.3,intv=21"));
    }
}

TEST_P(KKVReaderTest, TestLookupWhenDeleteAllSKey)
{
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());

    std::string docs = "cmd=add,pkey=1,skey=1,value=1,ts=1,locator=0:1;"
                       "cmd=add,pkey=2,skey=2,value=2,ts=2,locator=0:2;"
                       "cmd=add,pkey=1,skey=12,value=12,ts=3,locator=0:3;"
                       "cmd=delete,pkey=1,skey=1;"
                       "cmd=delete,pkey=1,skey=12;";

    ASSERT_TRUE(helper.Build(docs).IsOK());
    ASSERT_TRUE(helper.Query("kkv", "pkey", "1", ""));
}

TEST_P(KKVReaderTest, TestSameKeyAndDelete)
{
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());

    std::string docs = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;"
                       "cmd=add,pkey=2,skey=2,value=2,ts=1000000;"
                       "cmd=delete,pkey=1,ts=1000020;"
                       "cmd=add,pkey=2,skey=2,value=21,ts=3000000;";
    {
        ASSERT_TRUE(helper.Build(docs).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "999", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=21"));
    }

    {
        ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "999", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=21"));
    }
}

TEST_P(KKVReaderTest, TestDuplicateSKeyInMultiSegments)
{
    std::string tableOptionsJsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3,
            "building_memory_limit_mb": 4
        }
    }
    } )";
    PrepareTableOptions(tableOptionsJsonStr);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());

    std::string docs1 = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;"
                        "cmd=add,pkey=1,skey=12,value=12,ts=1000000;"
                        "cmd=add,pkey=2,skey=2,value=2,ts=1000000;"
                        "cmd=add,pkey=2,skey=22,value=22,ts=1000000;";
    std::string docs2 = "cmd=add,pkey=1,skey=-1,value=222,ts=1000000;"
                        "cmd=add,pkey=1,skey=1,value=2,ts=1000000;"
                        "cmd=add,pkey=2,skey=22,value=221,ts=1000000;"
                        "cmd=add,pkey=4,skey=-2,value=4,ts=1000000;"
                        "cmd=add,pkey=2,skey=22,value=222,ts=1000000;";
    std::string docs3 = "cmd=add,pkey=1,skey=1,value=121,ts=1000020;"
                        "cmd=delete,pkey=2,skey=2,ts=1000020;"
                        "cmd=add,pkey=4,skey=-2,value=5,ts=1000000;";

    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 0);
    ASSERT_TRUE(helper.BuildSegment(docs1).IsOK());
    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 1);
    ASSERT_TRUE(helper.BuildSegment(docs2).IsOK());
    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 2);
    ASSERT_TRUE(helper.BuildSegment(docs3).IsOK());
    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 3);

    ASSERT_TRUE(helper.Query("kkv", "pkey", "999", ""));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,value=121;skey=-1,value=222;skey=12,value=12;"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=22,value=222"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=-2,value=5"));
}

TEST_P(KKVReaderTest, TestDeleteDocInMultiSegment)
{
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());

    std::string docs1 = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;"
                        "cmd=add,pkey=1,skey=12,value=12,ts=1000000;"
                        "cmd=add,pkey=2,skey=2,value=2,ts=1000000;"
                        "cmd=add,pkey=2,skey=22,value=22,ts=1000000;";
    std::string docs2 = "cmd=add,pkey=3,skey=3,value=3,ts=1000000;"
                        "cmd=add,pkey=4,skey=4,value=4,ts=1000000;";
    std::string docs3 = "cmd=delete,pkey=1,ts=1000020;"         // delete segment_1 pkey
                        "cmd=delete,pkey=2,skey=2,ts=1000020;"; // delete segment_1 skey
    std::string docs4 = "cmd=delete,pkey=3,ts=1000020;"         // delete pkey and add again
                        "cmd=add,pkey=3,skey=33,value=33,ts=1000000;"
                        "cmd=delete,pkey=4,skey=4,ts=1000000;" // delete skey and add again
                        "cmd=add,pkey=4,skey=4,value=44,ts=1000000;";

    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 0);
    ASSERT_TRUE(helper.BuildSegment(docs1).IsOK());
    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 1);
    ASSERT_TRUE(helper.BuildSegment(docs2).IsOK());
    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 2);
    ASSERT_TRUE(helper.BuildSegment(docs3).IsOK());
    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 3);
    ASSERT_TRUE(helper.BuildSegment(docs4).IsOK());
    ASSERT_EQ(helper.GetCurrentVersion().GetSegmentCount(), 4);

    ASSERT_TRUE(helper.Query("kkv", "pkey", "999", ""));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "1", ""));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=22,value=22"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=33,value=33"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=44"));
}

TEST_P(KKVReaderTest, TestTTL)
{
    PrepareSchema("kkv_schema_ttl.json");
    ASSERT_TRUE(_indexConfig->TTLEnabled());

    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    // default ttl = 100
    std::string docs = "cmd=add,pkey=1,skey=1,value=1,ts=10000000;"  // 10
                       "cmd=add,pkey=2,skey=2,value=2,ts=20000000;"  // 20
                       "cmd=add,pkey=3,skey=3,value=3,ts=30000000;"; // 30
    ASSERT_TRUE(helper.Build(docs).IsOK());
    CheckTTLResult();
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    CheckTTLResult();
}

TEST_P(KKVReaderTest, TestTTLFromDoc)
{
    PrepareSchema("kkv_schema_ttl_from_doc.json");
    ASSERT_TRUE(_indexConfig->TTLEnabled());

    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    // default ttl = 300
    std::string docs = "cmd=add,pkey=1,skey=1,value=1,ts=10000000,field_for_ttl=100;"
                       "cmd=add,pkey=2,skey=2,value=2,ts=20000000,field_for_ttl=200;"
                       "cmd=add,pkey=3,skey=3,value=3,ts=10000000;"; // use default ttl
    ASSERT_TRUE(helper.Build(docs).IsOK());
    CheckTTLFromDocResult();
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    CheckTTLFromDocResult();
}

TEST_P(KKVReaderTest, TestLookupWithoutAnyDoc)
{
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());

    ASSERT_TRUE(helper.Query("kkv", "pkey", "1", ""));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "2", ""));
}

TEST_P(KKVReaderTest, TestBatchQuery)
{
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());

    std::string docs = "cmd=add,pkey=1,skey=1,value=1,ts=1000000;"
                       "cmd=add,pkey=2,skey=2,value=2,ts=1000000;"
                       "cmd=add,pkey=2,skey=3,value=3,ts=3000000;";
    {
        ASSERT_TRUE(helper.Build(docs).IsOK());
        indexlibv2::table::KKVReadOptions readOptions;
        ASSERT_TRUE(helper.DoBatchQuery("kkv", "pkey", {"1", "2"}, readOptions,
                                        "skey=1,value=1;skey=2,value=2;skey=3,value=3", true));
        // TODO(xinfei.sxf) fix this metrics
#if FUTURE_LITE_USE_COROUTINES
        ASSERT_EQ(helper.GetMetricsCollector()->GetMemTableCount(), 3);
        ASSERT_EQ(helper.GetMetricsCollector()->GetSSTableCount(), 0);
#else
        ASSERT_EQ(helper.GetMetricsCollector()->GetMemTableCount(), 0);
        ASSERT_EQ(helper.GetMetricsCollector()->GetSSTableCount(), 3);
#endif

        ASSERT_TRUE(helper.DoBatchQuery("kkv", "pkey", {"1", "2", "3"}, readOptions,
                                        "skey=1,value=1;skey=2,value=2;skey=3,value=3", true));
        // TODO(xinfei.sxf) fix this metrics
#if FUTURE_LITE_USE_COROUTINES
        ASSERT_EQ(helper.GetMetricsCollector()->GetMemTableCount(), 3);
        ASSERT_EQ(helper.GetMetricsCollector()->GetSSTableCount(), 0);
#else
        ASSERT_EQ(helper.GetMetricsCollector()->GetMemTableCount(), 0);
        ASSERT_EQ(helper.GetMetricsCollector()->GetSSTableCount(), 3);
#endif

        ASSERT_TRUE(helper.DoBatchQuery("kkv", "pkey", {"1", "2", "3"}, readOptions,
                                        "skey=1,value=1;skey=2,value=2;skey=3,value=3", false));
#if FUTURE_LITE_USE_COROUTINES
        ASSERT_EQ(helper.GetMetricsCollector()->GetMemTableCount(), 3);
        ASSERT_EQ(helper.GetMetricsCollector()->GetSSTableCount(), 0);
#else
        ASSERT_EQ(helper.GetMetricsCollector()->GetMemTableCount(), 0);
        ASSERT_EQ(helper.GetMetricsCollector()->GetSSTableCount(), 3);
#endif
    }

    // no metrics
    {
        ASSERT_TRUE(helper.Build(docs).IsOK());
        helper.DestructMetricsCollector();
        indexlibv2::table::KKVReadOptions readOptions;
        ASSERT_TRUE(helper.DoBatchQuery("kkv", "pkey", {"1", "2"}, readOptions,
                                        "skey=1,value=1;skey=2,value=2;skey=3,value=3", true));
        ASSERT_EQ(helper.GetMetricsCollector(), nullptr);
        ASSERT_TRUE(helper.DoBatchQuery("kkv", "pkey", {"1", "2", "3"}, readOptions,
                                        "skey=1,value=1;skey=2,value=2;skey=3,value=3", true));
        ASSERT_EQ(helper.GetMetricsCollector(), nullptr);
    }
}

TEST_P(KKVReaderTest, TestReaderImplShardCount)
{
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    std::string docs = "cmd=add,pkey=1,skey=1,value=1,ts=1,locator=0:1;"
                       "cmd=add,pkey=2,skey=2,value=2,ts=2,locator=0:2;"
                       "cmd=add,pkey=1,skey=12,value=12,ts=3,locator=0:3;"
                       "cmd=add,pkey=1,skey=13,value=13,ts=4,locator=0:4;"
                       "cmd=add,pkey=3,skey=3,value=3,ts=5,locator=0:5;"
                       "cmd=add,pkey=3,skey=31,value=31,ts=6,locator=0:6;"
                       "cmd=add,pkey=4,skey=4,value=4,ts=7,locator=0:7;"
                       "cmd=add,pkey=5,skey=5,value=5,ts=8,locator=0:8;"
                       "cmd=delete,pkey=2,ts=1000,locator=0:9;"
                       "cmd=delete,pkey=3,skey=31,ts=1000,locator=0:10;"
                       "cmd=delete,pkey=4,ts=1000,locator=0:11;";

    auto status = helper.Build(docs);
    ASSERT_TRUE(status.IsOK()) << status.ToString();

    auto reader = helper.GetTabletReader();
    ASSERT_TRUE(reader);
    auto kkvReader = reader->GetIndexReader("kkv", "pkey");
    ASSERT_TRUE(kkvReader);

    auto kkvReaderImpl = std::dynamic_pointer_cast<KKVReaderImpl<int16_t>>(kkvReader);
    ASSERT_TRUE(kkvReaderImpl);
    ASSERT_EQ(kkvReaderImpl->TEST_GetMemoryShardReaders().size(), 2);
    ASSERT_EQ(kkvReaderImpl->TEST_GetDiskShardReaders().size(), 2);
}

TEST_P(KKVReaderTest, TestCreateNormalKKVIteratorImpl)
{
    using BuildingSegmentReaderTyped = KKVBuildingSegmentReader<int32_t>;
    using BuildingSegmentReaderPtr = std::shared_ptr<BuildingSegmentReaderTyped>;
    using MemShardReaderAndLocator = std::pair<BuildingSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using BuiltSegmentReaderTyped = KKVBuiltSegmentReader<int32_t>;
    using BuiltSegmentReaderPtr = std::shared_ptr<BuiltSegmentReaderTyped>;
    using DiskShardReaderAndLocator = std::pair<BuiltSegmentReaderPtr, std::shared_ptr<framework::Locator>>;

    auto indexConfig = _indexConfig.get();
    uint64_t ttl = 200;
    PKeyType pkey = 0;
    std::vector<uint64_t> suffixKeyHashVec;
    std::vector<MemShardReaderAndLocator> buildingSegReaders;
    std::vector<DiskShardReaderAndLocator> builtSegReaders;
    uint64_t currentTimeInSecond = 100;

    using TableType = ClosedHashPrefixKeyTable<SKeyListInfo, PKeyTableType::DENSE>;
    auto fakeTable = std::make_shared<TableType>(1024 * 32, 50);
    fakeTable->Open(nullptr, PKeyTableOpenType::RW);
    auto fakeSKeyWriter = std::make_shared<SKeyWriter<int32_t>>();
    auto fakeValueWriter = std::make_shared<KKVValueWriter>(false);
    ASSERT_TRUE(fakeValueWriter->Init(32 * 1024 * 1024, 1).IsOK());
    auto fakeReader1 =
        std::make_shared<KKVBuildingSegmentReader<int32_t>>(_indexConfig, fakeTable, fakeSKeyWriter, fakeValueWriter);
    auto fakeLocator = std::make_shared<framework::Locator>();
    buildingSegReaders.push_back(std::make_pair(fakeReader1, fakeLocator));

    NormalKKVIteratorImpl<int32_t> iter(_pool.get(), indexConfig, ttl, pkey, suffixKeyHashVec, builtSegReaders,
                                        buildingSegReaders, currentTimeInSecond);
}

} // namespace indexlibv2::table
