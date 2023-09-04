#include "indexlib/table/kkv_table/KKVTabletWriter.h"

#include "autil/TimeUtility.h"
#include "future_lite/TaskScheduler.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/table/kkv_table/KKVTabletFactory.h"
#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"
#include "indexlib/table/plain/PlainMemSegment.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::index_base;

using namespace indexlibv2::framework;
using namespace indexlibv2::table;
using namespace indexlibv2::plain;
using namespace indexlibv2::document;

namespace indexlibv2::table {

class KKVTabletWriterTest : public TESTBASE
{
public:
    KKVTabletWriterTest() = default;
    ~KKVTabletWriterTest() = default;

public:
    void setUp() override;
    void tearDown() override;
    std::shared_ptr<framework::TabletData> PrepareTabletData();
    std::shared_ptr<IDocumentBatch> CreateDocumentBatch(const std::string& docStrings)
    {
        return document::KVDocumentBatchMaker::Make(_schema, docStrings);
    }

    void PrepareSchema(const std::string& dir, const std::string& schemaFileName);

private:
    mem_pool::Pool* _pool = nullptr;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::TabletOptions> _tabletOptions;
    std::shared_ptr<config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<KKVTabletFactory> _factory;
    BuildResource _resource;
    std::shared_ptr<framework::MetricsManager> _metricsManager;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KKVTabletWriterTest);

void KKVTabletWriterTest::setUp()
{
    setenv("DISABLE_CODEGEN", "true", 1);
    _pool = new mem_pool::Pool();
    // prepare buildResource
    kmonitor::MetricsTags metricsTags;
    _metricsManager = std::make_shared<framework::MetricsManager>(
        "", std::make_shared<kmonitor::MetricsReporter>("", metricsTags, ""));

    _resource.memController =
        std::make_shared<MemoryQuotaController>("", /*totalQuota=*/std::numeric_limits<int64_t>::max());
    _resource.metricsManager = _metricsManager.get();
    _resource.buildingMemLimit = 32 * 1024 * 1024;
    _resource.counterMap = std::make_shared<CounterMap>();
    _resource.buildResourceMetrics = make_shared<indexlib::util::BuildResourceMetrics>();
    // init root dir
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::IDirectory::Get(fs);
}

void KKVTabletWriterTest::tearDown()
{
    DELETE_AND_SET_NULL(_pool);
    unsetenv("DISABLE_CODEGEN");
}

std::shared_ptr<TabletData> KKVTabletWriterTest::PrepareTabletData()
{
    auto tabletName = "test";
    std::shared_ptr<TabletData> tabletData = std::make_shared<TabletData>(tabletName);
    SegmentMeta segMeta;
    segMeta.segmentId = 0;
    segMeta.schema = _schema;
    segMeta.segmentDir = indexlib::file_system::IDirectory::ToLegacyDirectory(
        _rootDir->MakeDirectory("segment_0_level_0", indexlib::file_system::DirectoryOption::Mem()).GetOrThrow());
    segMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
    Locator locator1(0, 1024);
    segMeta.segmentInfo->SetLocator(locator1);

    std::string tableOptionsJsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3,
            "building_memory_limit_mb": 4,
            "max_doc_count": 4
        }
    }
    } )";

    _tabletOptions = KKVTableTestHelper::CreateTableOptions(tableOptionsJsonStr);

    _factory = std::make_shared<KKVTabletFactory>();
    _factory->Init(_tabletOptions, nullptr);

    std::shared_ptr<MemSegment> buildingSeg(_factory->CreateMemSegment(segMeta).release());
    auto status = buildingSeg->Open(_resource, nullptr);
    if (!status.IsOK()) {
        return nullptr;
    }
    std::vector<std::shared_ptr<Segment>> allSegments;
    allSegments.emplace_back(buildingSeg);

    framework::Version onDiskVersion;
    auto schemaGroup = std::make_shared<TabletDataSchemaGroup>();
    schemaGroup->writeSchema = _schema;
    schemaGroup->onDiskReadSchema = _schema;
    schemaGroup->onDiskWriteSchema = _schema;
    schemaGroup->multiVersionSchemas[0] = _schema;
    auto resourceMap = std::make_shared<framework::ResourceMap>();
    auto s = resourceMap->AddVersionResource(TabletDataSchemaGroup::NAME, schemaGroup);
    if (!s.IsOK()) {
        return nullptr;
    }
    s = tabletData->Init(std::move(onDiskVersion), allSegments, resourceMap);
    if (!s.IsOK()) {
        return nullptr;
    }
    return tabletData;
}

void KKVTabletWriterTest::PrepareSchema(const std::string& dir, const std::string& schemaFileName)
{
    _schema = KKVTableTestHelper::LoadSchema(dir, schemaFileName);
    _indexConfig = KKVTableTestHelper::GetIndexConfig(_schema, "pkey");
}

TEST_F(KKVTabletWriterTest, TestBuildIndexForSingleKKV)
{
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), "kkv_schema_single_value.json");
    auto tabletData = PrepareTabletData();
    std::shared_ptr<KKVTabletWriter> writer = make_shared<KKVTabletWriter>(_schema, _tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());
    string docString = "cmd=add,pkey=1,skey=1,floatmv=1.1 1.2 1.3,ts=1;"
                       "cmd=add,pkey=2,skey=2,floatmv=2.1 2.2 2.3,ts=2;"
                       "cmd=add,pkey=2,skey=21,floatmv=21.1 21.2 21.3,ts=2;";

    auto docBatch = CreateDocumentBatch(docString);
    auto s = writer->Build(docBatch);
    ASSERT_TRUE(s.IsOK());

    KKVReadOptions readOptions;
    readOptions.pool = _pool;
    auto kkvReader = writer->GetKKVReader(0);
    ASSERT_TRUE(kkvReader);

    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.DoQuery(kkvReader, _indexConfig, "1", "skey=1,floatmv=1.1 1.2 1.3"));
    ASSERT_TRUE(
        helper.DoQuery(kkvReader, _indexConfig, "2", "skey=2,floatmv=2.1 2.2 2.3;skey=21,floatmv=21.1 21.2 21.3"));
}

TEST_F(KKVTabletWriterTest, TestBuildIndexForPackAttr)
{
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), "kkv_schema_pack_value.json");
    auto tabletData = PrepareTabletData();
    shared_ptr<KKVTabletWriter> writer = make_shared<KKVTabletWriter>(_schema, _tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());
    string docString = "cmd=add,pkey=1,skey=1,intv=1,floatmv=1.1 1.2 1.3,ts=1;"
                       "cmd=add,pkey=2,skey=2,intv=2,floatmv=2.1 2.2 2.3,ts=2;"
                       "cmd=add,pkey=2,skey=21,intv=21,floatmv=21.1 21.2 21.3,ts=2;";
    auto docBatch = CreateDocumentBatch(docString);
    auto s = writer->Build(docBatch);
    ASSERT_TRUE(s.IsOK());

    KKVReadOptions readOptions;
    readOptions.pool = _pool;
    auto kkvReader = writer->GetKKVReader(0);
    ASSERT_TRUE(kkvReader);

    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.DoQuery(kkvReader, _indexConfig, "1", "skey=1,intv=1,floatmv=1.1 1.2 1.3"));
    ASSERT_TRUE(helper.DoQuery(kkvReader, _indexConfig, "2",
                               "skey=2,intv=2,floatmv=2.1 2.2 2.3;skey=21,intv=21,floatmv=21.1 21.2 21.3"));
}

TEST_F(KKVTabletWriterTest, TestBuildEmptySKey)
{
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), "kkv_schema_single_value.json");
    auto tabletData = PrepareTabletData();
    std::shared_ptr<KKVTabletWriter> writer = make_shared<KKVTabletWriter>(_schema, _tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());

    string docString0 = "cmd=add,pkey=1,skey=,floatmv=1 1 1,ts=0;";
    auto docBatch0 = CreateDocumentBatch(docString0);
    ASSERT_TRUE(writer->Build(docBatch0).IsOK());

    string docString1 = "cmd=add,pkey=1,skey=2,floatmv=2 2 2,ts=0;";
    auto docBatch1 = CreateDocumentBatch(docString1);
    ASSERT_TRUE(writer->Build(docBatch1).IsOK());

    string docString2 = "cmd=add,pkey=1,skey=,floatmv=3 3 3,ts=0;";
    auto docBatch2 = CreateDocumentBatch(docString2);
    _indexConfig->SetDenyEmptySuffixKey(true);
    ASSERT_FALSE(writer->Build(docBatch2).IsOK());

    KKVReadOptions readOptions;
    readOptions.pool = _pool;
    auto kkvReader = writer->GetKKVReader(0);
    ASSERT_TRUE(kkvReader);

    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.DoQuery(kkvReader, _indexConfig, "1", "skey=2,floatmv=2 2 2"));
}

TEST_F(KKVTabletWriterTest, TestBuildEmptyValue)
{
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), "kkv_schema_single_value.json");
    auto tabletData = PrepareTabletData();
    std::shared_ptr<KKVTabletWriter> writer = make_shared<KKVTabletWriter>(_schema, _tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());

    string docString0 = "cmd=add,pkey=1,skey=1,floatmv=,ts=0;";
    auto docBatch0 = CreateDocumentBatch(docString0);
    ASSERT_EQ(1, docBatch0->GetBatchSize());
    ASSERT_TRUE(writer->Build(docBatch0).IsOK());

    string docString1 = "cmd=add,pkey=1,skey=2,floatmv=2 2 2,ts=0;";
    auto docBatch1 = CreateDocumentBatch(docString1);
    ASSERT_TRUE(writer->Build(docBatch1).IsOK());

    KKVReadOptions readOptions;
    readOptions.pool = _pool;
    auto kkvReader = writer->GetKKVReader(0);
    ASSERT_TRUE(kkvReader);

    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.DoQuery(kkvReader, _indexConfig, "1", "skey=1,floatmv=0 0 0;skey=2,floatmv=2 2 2"));
}

} // namespace indexlibv2::table
