#include "indexlib/table/kv_table/KVTabletWriter.h"

#include "autil/TimeUtility.h"
#include "future_lite/TaskScheduler.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/IndexConfigHash.h"
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
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/kv_table/KVTabletFactory.h"
#include "indexlib/table/plain/PlainMemSegment.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

using namespace indexlibv2::framework;
using namespace indexlibv2::table;
using namespace indexlibv2::plain;
using namespace indexlibv2::document;
using namespace indexlibv2::index;

namespace indexlibv2::table {

class KVTabletWriterTest : public TESTBASE
{
public:
    KVTabletWriterTest() = default;
    ~KVTabletWriterTest() = default;

public:
    void setUp() override;
    void tearDown() override;
    std::shared_ptr<framework::TabletData> PrepareTabletData();
    std::shared_ptr<IDocumentBatch>
    CreateDocumentBatch(const std::string& docStrings,
                        std::shared_ptr<indexlibv2::config::ITabletSchema> schema = nullptr)
    {
        if (!schema) {
            schema = _schema;
        }
        return document::KVDocumentBatchMaker::Make(schema, docStrings);
    }

    void PrepareSchema(const std::string& dir, const std::string& schemaFileName,
                       const std::shared_ptr<config::TabletOptions>& tabletOptions);
    void PrepareReaderForAddPKValue(const std::string& schemaName, index::KVIndexReaderPtr& pkValueReader,
                                    index::KVIndexReaderPtr& indexReader);
    void CheckIndexReader(const KVReadOptions& options, index::KVIndexReaderPtr& indexReader);

private:
    mem_pool::Pool* _pool = nullptr;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::unique_ptr<future_lite::Executor> _executor = nullptr;
    std::unique_ptr<future_lite::TaskScheduler> _taskScheduler = nullptr;
    shared_ptr<indexlibv2::config::TabletOptions> _tabletOptions;
    shared_ptr<KVTabletFactory> _factory;
    BuildResource _resource;
    std::shared_ptr<framework::MetricsManager> _metricsManager;

private:
    AUTIL_LOG_DECLARE();
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

AUTIL_LOG_SETUP(indexlib.index, KVTabletWriterTest);

void KVTabletWriterTest::setUp()
{
    setenv("DISABLE_CODEGEN", "true", 1);
    _pool = new mem_pool::Pool();
    // prepare executor
    _executor.reset(new future_lite::executors::SimpleExecutor(2));
    _taskScheduler.reset(new future_lite::TaskScheduler(_executor.get()));
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
    _tabletOptions = std::make_shared<config::TabletOptions>();
    // init root dir
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(fs);
}
void KVTabletWriterTest::tearDown()
{
    DELETE_AND_SET_NULL(_pool);
    unsetenv("DISABLE_CODEGEN");
}

std::shared_ptr<TabletData> KVTabletWriterTest::PrepareTabletData()
{
    auto tabletName = "test";
    std::shared_ptr<TabletData> tabletData = std::make_shared<TabletData>(tabletName);
    SegmentMeta segMeta;
    segMeta.segmentId = 0;
    segMeta.schema = _schema;
    segMeta.segmentDir = _rootDir->MakeDirectory("segment_0_level_0");
    segMeta.segmentInfo = std::make_shared<framework::SegmentInfo>();
    Locator locator1(0, 1024);
    segMeta.segmentInfo->SetLocator(locator1);

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

void KVTabletWriterTest::PrepareSchema(const std::string& dir, const std::string& schemaFileName,
                                       const std::shared_ptr<config::TabletOptions>& tabletOptions)
{
    _factory = std::make_shared<KVTabletFactory>();
    _factory->Init(tabletOptions, nullptr);
    shared_ptr<config::TabletSchema> schema = framework::TabletSchemaLoader::LoadSchema(dir, schemaFileName);
    ASSERT_TRUE(schema);
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(tabletOptions, "", schema.get()).IsOK());
    _schema = schema;
}

void KVTabletWriterTest::PrepareReaderForAddPKValue(const std::string& schemaName,
                                                    index::KVIndexReaderPtr& pkValueReader,
                                                    index::KVIndexReaderPtr& indexReader)
{
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsLeader(true);
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), schemaName, _tabletOptions);
    auto indexConfigs = _schema->GetIndexConfigs();
    ASSERT_EQ(size_t(2), indexConfigs.size());
    std::shared_ptr<config::KVIndexConfig> indexConfig;
    std::shared_ptr<config::KVIndexConfig> pkValueIndexConfig;
    for (auto config : indexConfigs) {
        if (index::KV_RAW_KEY_INDEX_NAME == config->GetIndexName()) {
            pkValueIndexConfig = dynamic_pointer_cast<config::KVIndexConfig>(config);
        } else {
            indexConfig = dynamic_pointer_cast<config::KVIndexConfig>(config);
        }
    }
    ASSERT_TRUE(indexConfig);
    ASSERT_TRUE(pkValueIndexConfig);
    auto tabletData = PrepareTabletData();
    shared_ptr<KVTabletWriter> writer = make_shared<KVTabletWriter>(_schema, tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());
    indexReader = writer->GetKVReader(config::IndexConfigHash::Hash(indexConfig));
    ASSERT_TRUE(indexReader);
    pkValueReader = writer->GetKVReader(config::IndexConfigHash::Hash(pkValueIndexConfig));
    ASSERT_TRUE(pkValueReader);
    string docString = "cmd=add,key=1,weights1=1.1 1.2 1.3,weights2=1.4 1.5 1.6,ts=100000000;"
                       "cmd=add,key=23,weights1=2.1 2.2 2.3,weights2=2.4 2.5 2.6,ts=100000000;";
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), schemaName, _tabletOptions);
    auto docBatch = CreateDocumentBatch(docString);
    auto s = writer->Build(docBatch);
    ASSERT_TRUE(s.IsOK());
}

void KVTabletWriterTest::CheckIndexReader(const KVReadOptions& options, index::KVIndexReaderPtr& indexReader)
{
    auto result = indexReader->Get(autil::StringView("23"), options);
    ASSERT_EQ(KVResultStatus::FOUND, result.status);
    autil::MultiFloat value;
    result.valueExtractor.GetTypedValue("weights1", value);
    ASSERT_EQ(3, value.size());
    ASSERT_EQ(float(2.1), value[0]);
    ASSERT_EQ(float(2.2), value[1]);
    ASSERT_EQ(float(2.3), value[2]);
    autil::MultiFloat value2;
    result.valueExtractor.GetTypedValue("weights2", value2);
    ASSERT_EQ(3, value2.size());
    ASSERT_EQ(float(2.4), value2[0]);
    ASSERT_EQ(float(2.5), value2[1]);
    ASSERT_EQ(float(2.6), value2[2]);
}

TEST_F(KVTabletWriterTest, TestBuildIndexForSingleKV)
{
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), "single_kv_schema.json", _tabletOptions);
    auto tabletData = PrepareTabletData();
    shared_ptr<KVTabletWriter> writer = make_shared<KVTabletWriter>(_schema, _tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());
    auto kvReader = writer->GetKVReader(0);
    string docString = "cmd=add,key=1,weights=1.1 1.2 1.3,ts=100000000;"
                       "cmd=add,key=2,weights=2.1 2.2 2.3,ts=100000000;";
    KVReadOptions options;
    options.pool = _pool;
    options.timestamp = (uint64_t)TimeUtility::currentTime();
    auto docBatch = CreateDocumentBatch(docString);
    auto s = writer->Build(docBatch);
    ASSERT_TRUE(s.IsOK());

    {
        auto result = kvReader->Get(index::keytype_t(1), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(1.1), value[0]);
        ASSERT_EQ(float(1.2), value[1]);
        ASSERT_EQ(float(1.3), value[2]);
    }
    {
        auto result = kvReader->Get(index::keytype_t(2), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(2.1), value[0]);
        ASSERT_EQ(float(2.2), value[1]);
        ASSERT_EQ(float(2.3), value[2]);
    }
}

TEST_F(KVTabletWriterTest, TestBuildIndexForMultiKV)
{
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), "multi_kv_schema.json", _tabletOptions);
    auto tabletData = PrepareTabletData();
    shared_ptr<KVTabletWriter> writer = make_shared<KVTabletWriter>(_schema, _tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());

    auto indexNameHash1 = config::IndexConfigHash::Hash(_schema->GetIndexConfig("kv", "index1"));
    auto indexNameHash2 = config::IndexConfigHash::Hash(_schema->GetIndexConfig("kv", "index2"));
    stringstream docs;
    docs << "cmd=add,weights=1.1 1.2 1.3,key1=1,ts=10000000;"
         << "cmd=add,weights=1.11 1.22 1.33,key1=2,ts=20000000;"
         << "cmd=add,weights=2.1 2.2 2.3,key2=1,ts=10000000;"
         << "cmd=add,weights=2.11 2.22 2.33,key2=2,ts=20000000;";

    KVReadOptions options;
    options.pool = _pool;
    options.timestamp = (uint64_t)TimeUtility::currentTime();
    auto docBatch = CreateDocumentBatch(docs.str());
    auto s = writer->Build(docBatch);
    ASSERT_TRUE(s.IsOK());
    {
        auto kvReader = writer->GetKVReader(indexNameHash1);
        auto result = kvReader->Get(index::keytype_t(0), options);
        ASSERT_EQ(KVResultStatus::NOT_FOUND, result.status);
        result = kvReader->Get(index::keytype_t(1), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(1.1), value[0]);
        ASSERT_EQ(float(1.2), value[1]);
        ASSERT_EQ(float(1.3), value[2]);
    }
    {
        auto kvReader = writer->GetKVReader(indexNameHash1);
        auto result = kvReader->Get(index::keytype_t(2), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(1.11), value[0]);
        ASSERT_EQ(float(1.22), value[1]);
        ASSERT_EQ(float(1.33), value[2]);
    }
    {
        auto kvReader = writer->GetKVReader(indexNameHash2);
        auto result = kvReader->Get(index::keytype_t(0), options);
        ASSERT_EQ(KVResultStatus::NOT_FOUND, result.status);
        result = kvReader->Get(index::keytype_t(1), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(2.1), value[0]);
        ASSERT_EQ(float(2.2), value[1]);
        ASSERT_EQ(float(2.3), value[2]);
    }
    {
        auto kvReader = writer->GetKVReader(indexNameHash2);
        auto result = kvReader->Get(index::keytype_t(2), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(2.11), value[0]);
        ASSERT_EQ(float(2.22), value[1]);
        ASSERT_EQ(float(2.33), value[2]);
    }
}

TEST_F(KVTabletWriterTest, TestBuildIndexForLegacyKV)
{
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), "legacy_kv_schema.json", _tabletOptions);
    auto tabletData = PrepareTabletData();
    shared_ptr<KVTabletWriter> writer = make_shared<KVTabletWriter>(_schema, _tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());
    auto kvReader = writer->GetKVReader(0);
    string docString = "cmd=add,key=1,weights=1.1 1.2 1.3,ts=100000000;"
                       "cmd=add,key=2,weights=2.1 2.2 2.3,ts=100000000;";
    KVReadOptions options;
    options.pool = _pool;
    options.timestamp = (uint64_t)TimeUtility::currentTime();
    auto docBatch = CreateDocumentBatch(docString);
    auto s = writer->Build(docBatch);
    ASSERT_TRUE(s.IsOK());

    {
        auto result = kvReader->Get(index::keytype_t(1), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(1.1), value[0]);
        ASSERT_EQ(float(1.2), value[1]);
        ASSERT_EQ(float(1.3), value[2]);
    }
    {
        auto result = kvReader->Get(index::keytype_t(2), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(2.1), value[0]);
        ASSERT_EQ(float(2.2), value[1]);
        ASSERT_EQ(float(2.3), value[2]);
    }
}

TEST_F(KVTabletWriterTest, TestBuildIndexForPackAttr)
{
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), "pack_kv_schema.json", _tabletOptions);
    auto tabletData = PrepareTabletData();
    shared_ptr<KVTabletWriter> writer = make_shared<KVTabletWriter>(_schema, _tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());
    uint64_t hash = 0;
    for (auto indexConfig : _schema->GetIndexConfigs()) {
        if (index::KV_RAW_KEY_INDEX_NAME == indexConfig->GetIndexName()) {
            continue;
        }
        hash = config::IndexConfigHash::Hash(indexConfig);
    }
    auto kvReader = writer->GetKVReader(hash);
    string docString = "cmd=add,key=1,weights1=1.1 1.2 1.3,weights2=1.4 1.5 1.6,ts=100000000;"
                       "cmd=add,key=2,weights1=2.1 2.2 2.3,weights2=2.4 2.5 2.6,ts=100000000;";
    KVReadOptions options;
    options.pool = _pool;
    options.timestamp = (uint64_t)TimeUtility::currentTime();
    auto docBatch = CreateDocumentBatch(docString);
    auto s = writer->Build(docBatch);
    ASSERT_TRUE(s.IsOK());
    {
        auto result = kvReader->Get(index::keytype_t(1), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights1", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(1.1), value[0]);
        ASSERT_EQ(float(1.2), value[1]);
        ASSERT_EQ(float(1.3), value[2]);

        autil::MultiFloat value2;
        result.valueExtractor.GetTypedValue("weights2", value2);
        ASSERT_EQ(3, value2.size());
        ASSERT_EQ(float(1.4), value2[0]);
        ASSERT_EQ(float(1.5), value2[1]);
        ASSERT_EQ(float(1.6), value2[2]);
    }
    {
        auto result = kvReader->Get(index::keytype_t(2), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        autil::MultiFloat value;
        result.valueExtractor.GetTypedValue("weights1", value);
        ASSERT_EQ(3, value.size());
        ASSERT_EQ(float(2.1), value[0]);
        ASSERT_EQ(float(2.2), value[1]);
        ASSERT_EQ(float(2.3), value[2]);

        autil::MultiFloat value2;
        result.valueExtractor.GetTypedValue("weights2", value2);
        ASSERT_EQ(3, value2.size());
        ASSERT_EQ(float(2.4), value2[0]);
        ASSERT_EQ(float(2.5), value2[1]);
        ASSERT_EQ(float(2.6), value2[2]);
    }
}

TEST_F(KVTabletWriterTest, TestBuildIndexForAddPKValue)
{
    KVReadOptions options;
    options.pool = _pool;
    options.timestamp = (uint64_t)TimeUtility::currentTime();
    index::KVIndexReaderPtr pkValueReader;
    index::KVIndexReaderPtr indexReader;
    {
        PrepareReaderForAddPKValue("/pack_kv_schema.json", pkValueReader, indexReader);
        auto result = pkValueReader->Get(index::keytype_t(1), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        ASSERT_EQ(size_t(1), result.valueExtractor.GetFieldCount());
        int64_t keyValue = 0;
        ASSERT_TRUE(result.valueExtractor.GetTypedValue<int64_t>(0, keyValue));
        ASSERT_EQ(int64_t(1), keyValue);

        result = pkValueReader->Get(index::keytype_t(23), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        ASSERT_EQ(size_t(1), result.valueExtractor.GetFieldCount());
        ASSERT_TRUE(result.valueExtractor.GetTypedValue<int64_t>(0, keyValue));
        ASSERT_EQ(int64_t(23), keyValue);

        CheckIndexReader(options, indexReader);
    }
    {
        PrepareReaderForAddPKValue("/pack_kv_schema2.json", pkValueReader, indexReader);
        auto result = pkValueReader->Get(autil::StringView("1"), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        ASSERT_EQ(size_t(1), result.valueExtractor.GetFieldCount());
        string name;
        FieldType type;
        bool isMulti = false;
        ASSERT_TRUE(result.valueExtractor.GetValueMeta(0, name, type, isMulti));
        autil::MultiChar keyValue;
        ASSERT_TRUE(result.valueExtractor.GetTypedValue(0, keyValue));
        ASSERT_EQ(std::string("1"), std::string(keyValue.getData(), keyValue.getDataSize()));

        result = pkValueReader->Get(autil::StringView("23"), options);
        ASSERT_EQ(KVResultStatus::FOUND, result.status);
        ASSERT_EQ(size_t(1), result.valueExtractor.GetFieldCount());
        ASSERT_TRUE(result.valueExtractor.GetTypedValue<autil::MultiChar>(0, keyValue));
        ASSERT_EQ(std::string("23"), std::string(keyValue.getData(), keyValue.getDataSize()));

        CheckIndexReader(options, indexReader);
    }
}

TEST_F(KVTabletWriterTest, TestFilterDocumentWithSchemaId)
{
    PrepareSchema(GET_PRIVATE_TEST_DATA_PATH(), "pack_kv_schema3.json", _tabletOptions);
    auto tabletData = PrepareTabletData();
    shared_ptr<KVTabletWriter> writer = make_shared<KVTabletWriter>(_schema, _tabletOptions.get());
    auto status = writer->Open(tabletData, _resource, framework::OpenOptions());
    ASSERT_TRUE(status.IsOK());
    uint64_t hash = 0;
    for (auto indexConfig : _schema->GetIndexConfigs()) {
        if (index::KV_RAW_KEY_INDEX_NAME == indexConfig->GetIndexName()) {
            continue;
        }
        hash = config::IndexConfigHash::Hash(indexConfig);
    }
    auto kvReader = writer->GetKVReader(hash);
    string docString1 = "cmd=add,key=abc,weight=abc,weights1=1.1 2.2 2.3,weights2=2.4 1.5 2.6,ts=100000000;";
    KVReadOptions options;
    options.pool = _pool;
    options.timestamp = (uint64_t)TimeUtility::currentTime();
    shared_ptr<config::ITabletSchema> schema =
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "pack_kv_schema_version_diff.json");
    ASSERT_TRUE(schema);
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(_tabletOptions, "", schema.get()).IsOK());

    auto docBatch1 = CreateDocumentBatch(docString1, schema);
    for (auto i = 0; i < 10; i++) {
        auto s1 = writer->Build(docBatch1);
        ASSERT_FALSE(s1.IsOK());
    }
}

} // namespace indexlibv2::table
