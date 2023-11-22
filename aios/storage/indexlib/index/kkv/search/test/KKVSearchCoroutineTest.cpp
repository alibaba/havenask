
#include "indexlib/index/kkv/search/KKVSearchCoroutine.h"

#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/kkv/building/KKVMemIndexer.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentIterator.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentReader.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/search/SearchContext.h"
#include "indexlib/table/kkv_table/KKVSchemaResolver.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlibv2::base;
using namespace indexlibv2::framework;

namespace indexlibv2::index {

class KKVSearchCoroutineTest : public TESTBASE
{
private:
    using SKeyType = int32_t;
    using BuildingSegmentReaderTyped = KKVBuildingSegmentReader<SKeyType>;
    using BuildingSegmentReaderPtr = std::shared_ptr<BuildingSegmentReaderTyped>;
    using MemShardReaderAndLocator = std::pair<BuildingSegmentReaderPtr, std::shared_ptr<framework::Locator>>;
    using BuiltSegmentReaderTyped = KKVBuiltSegmentReader<SKeyType>;
    using BuiltSegmentReaderPtr = std::shared_ptr<BuiltSegmentReaderTyped>;
    using DiskShardReaderAndLocator = std::pair<BuiltSegmentReaderPtr, std::shared_ptr<framework::Locator>>;

public:
    void setUp() override;

private:
    void PrepareSchema();
    void PrepareReaders();
    void BuildDocs(KKVMemIndexer<int32_t>& memIndexer, const std::string& docStr);
    void CreateBuiltSegmentReader(const std::string& indexDirPath, const std::string& docStr, bool isRtSegment,
                                  std::shared_ptr<KKVBuiltSegmentReader<int32_t>>& result);
    base::Progress GetProgress(uint32_t from, uint32_t to, int64_t ts) { return {from, to, {ts, 0}}; }

private:
    future_lite::executors::SimpleExecutor _executor = {1};
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;

    std::string _docStr1;
    std::string _docStr2;
    std::string _docStr3;

    std::vector<MemShardReaderAndLocator> _buildingSegReaders;
    std::vector<DiskShardReaderAndLocator> _builtSegReaders;
};

void KKVSearchCoroutineTest::setUp()
{
    _pool.reset(new Pool());

    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::IDirectory::Get(fs);

    _docStr1 = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
               "cmd=add,pkey=1,skey=2,value=2,ts=200000000;"
               "cmd=add,pkey=2,skey=1,value=1,ts=300000000;"
               "cmd=add,pkey=4,skey=1,value=1,ts=500000000;";
    _docStr2 = "cmd=delete,pkey=1,skey=2,ts=400000000;"
               "cmd=add,pkey=1,skey=2,value=3,ts=500000000;"
               "cmd=delete,pkey=4,ts=500000000;"
               "cmd=delete,pkey=2,ts=600000000;";
    _docStr3 = "cmd=add,pkey=1,skey=2,value=4,ts=800000000;"
               "cmd=delete,pkey=2,ts=900000000;";
}

void KKVSearchCoroutineTest::PrepareSchema()
{
    string schemaJsonStr = R"({
        "attributes" : [ "value" ],
        "fields" : [
            {"field_type" : "UINT64", "field_name" : "pkey"},
            {"field_type" : "UINT64", "field_name" : "skey"},
            {"field_type" : "INT16", "field_name" : "value"}
        ],
        "indexs" : [ {
            "index_name" : "pkey_skey",
            "index_type" : "PRIMARY_KEY",
            "index_fields" :
                [ {"field_name" : "pkey", "key_type" : "prefix"}, {"field_name" : "skey", "key_type" : "suffix"} ],
            "value_fields" : [
                {"field_type" : "INT16", "field_name" : "value"}
            ]
        } ],
        "table_name" : "kkv_table",
        "table_type" : "kkv"
    })";

    _schema.reset(framework::TabletSchemaLoader::LoadSchema(schemaJsonStr).release());
    auto kkvIndexConfigs = _schema->GetIndexConfigs();
    ASSERT_EQ(1, kkvIndexConfigs.size());

    _indexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(kkvIndexConfigs[0]);
    ASSERT_NE(nullptr, _indexConfig);

    _indexConfig->GetIndexPreference().GetHashDictParam().SetHashType(KKV_PKEY_DENSE_TABLE_NAME);
    _indexConfig->SetUseNumberHash(true);
}

void KKVSearchCoroutineTest::PrepareReaders()
{
    // offline segment
    std::shared_ptr<KKVBuiltSegmentReader<int32_t>> builtReaderPtr1;
    CreateBuiltSegmentReader("segment_1", _docStr1, false, builtReaderPtr1);
    // dumped rt segment
    std::shared_ptr<KKVBuiltSegmentReader<int32_t>> builtReaderPtr2;
    CreateBuiltSegmentReader("segment_2", _docStr2, true, builtReaderPtr2);
    // building segment
    KKVMemIndexer<SKeyType> memIndexer("test", 1024, 1024, 1024, 1.0, 1.0, true, true);
    ASSERT_TRUE(memIndexer.Init(_indexConfig, nullptr).IsOK());
    BuildDocs(memIndexer, _docStr3);
    auto buildingReaderPtr1 = memIndexer.CreateInMemoryReader();

    auto locatorPtr1 = std::make_shared<framework::Locator>();
    locatorPtr1->SetMultiProgress({{GetProgress(0, 32767, 97), GetProgress(32768, 65535, 99)}});
    auto locatorPtr2 = std::make_shared<framework::Locator>();
    locatorPtr2->SetMultiProgress({{GetProgress(0, 32767, 101), GetProgress(32768, 65535, 103)}});
    auto locatorPtr3 = std::make_shared<framework::Locator>();
    locatorPtr3->SetMultiProgress({{GetProgress(0, 32767, 105), GetProgress(32768, 65535, 107)}});

    _builtSegReaders.emplace_back(std::make_pair(builtReaderPtr1, locatorPtr1));
    _builtSegReaders.emplace_back(std::make_pair(builtReaderPtr2, locatorPtr2));
    _buildingSegReaders.emplace_back(std::make_pair(buildingReaderPtr1, locatorPtr3));
}

void KKVSearchCoroutineTest::BuildDocs(KKVMemIndexer<int32_t>& memIndexer, const std::string& docStr)
{
    auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
    // Build
    for (const auto& rawDoc : rawDocs) {
        auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
        ASSERT_TRUE(docBatch);
        auto status = memIndexer.Build(docBatch.get());
        ASSERT_TRUE(status.IsOK());
    }
}

void KKVSearchCoroutineTest::CreateBuiltSegmentReader(const std::string& indexDirPath, const std::string& docStr,
                                                      bool isRtSegment,
                                                      std::shared_ptr<KKVBuiltSegmentReader<int32_t>>& result)
{
    uint32_t segTimestamp = 100;

    KKVMemIndexer<SKeyType> memIndexer("test", 1024, 1024, 1024, 1.0, 1.0, true, true);
    ASSERT_TRUE(memIndexer.Init(_indexConfig, nullptr).IsOK());
    BuildDocs(memIndexer, docStr);

    // Dump
    auto dumpPool = std::make_shared<autil::mem_pool::UnsafePool>(1024 * 1024);
    std::shared_ptr<framework::DumpParams> dumpParams;
    auto indexDirectory =
        _rootDir->MakeDirectory(indexDirPath, indexlib::file_system::DirectoryOption::Mem()).GetOrThrow();
    auto status = memIndexer.Dump(dumpPool.get(), indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory),
                                  dumpParams);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(indexDirectory->IsExist(_indexConfig->GetIndexName()).OK());
    auto ret = indexDirectory->GetDirectory(_indexConfig->GetIndexName());
    ASSERT_TRUE(ret.OK());
    auto kkvDir = ret.Value();
    ASSERT_TRUE(kkvDir);
    ASSERT_TRUE(kkvDir->IsExist("pkey").GetOrThrow());
    ASSERT_TRUE(kkvDir->IsExist("skey").GetOrThrow());
    ASSERT_TRUE(kkvDir->IsExist("value").GetOrThrow());

    // Load by KKVBuiltSegmentReader
    result.reset(new BuiltSegmentReaderTyped(segTimestamp, isRtSegment));
    ASSERT_TRUE(result->Open(_indexConfig, indexDirectory).IsOK());
}

TEST_F(KKVSearchCoroutineTest, TestSimple)
{
    PrepareSchema();
    PrepareReaders();

    PKeyType pkey = 1;
    int currentTsInSecond = 0;
    bool keepSortSeq = false;
    int skeyCountLimits = 1024;
    KKVMetricsCollector* metricsCollector = nullptr;

    {
        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);

        KKVDocs kkvDocs(_pool.get());
        auto status = KKVSearchCoroutine<SKeyType>::SearchBuilding(searchContext, kkvDocs);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(searchContext.hasPKeyDeleted);
        ASSERT_TRUE(searchContext.currentSKeyCount < skeyCountLimits);
        ASSERT_EQ(kkvDocs.size(), 1);
        ASSERT_EQ(kkvDocs[0].skey, 2);
        ASSERT_EQ(kkvDocs[0].timestamp, 800);

        ASSERT_EQ(searchContext.currentSKeyCount, 1);
        ASSERT_EQ(searchContext.seekSKeyCount, 1);
        ASSERT_EQ(searchContext.seekRtSegmentCount, 0);
        ASSERT_EQ(searchContext.seekSegmentCount, 0);

        status = future_lite::interface::syncAwait(KKVSearchCoroutine<SKeyType>::SearchBuilt(searchContext, kkvDocs),
                                                   &_executor);
        ASSERT_TRUE(status.IsOK());
        // we have one duplicated skey
        ASSERT_EQ(kkvDocs.size(), 3);
        ASSERT_EQ(kkvDocs[0].skey, 2);
        ASSERT_EQ(kkvDocs[0].timestamp, 800);
        ASSERT_EQ(kkvDocs[1].skey, 2);
        ASSERT_EQ(kkvDocs[1].timestamp, 500);
        ASSERT_TRUE(kkvDocs[1].duplicatedKey);
        ASSERT_EQ(kkvDocs[2].skey, 1);
        ASSERT_EQ(kkvDocs[2].timestamp, 100);
        ASSERT_FALSE(kkvDocs[2].duplicatedKey);

        ASSERT_EQ(searchContext.currentSKeyCount, 2);
        ASSERT_EQ(searchContext.seekSKeyCount, 4);
        ASSERT_EQ(searchContext.seekRtSegmentCount, 1);
        ASSERT_EQ(searchContext.seekSegmentCount, 2);
    }

    {
        PKeyType pkey = 2;
        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);

        KKVDocs kkvDocs(_pool.get());
        auto status = KKVSearchCoroutine<SKeyType>::SearchBuilding(searchContext, kkvDocs);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(searchContext.hasPKeyDeleted);
        ASSERT_EQ(searchContext.currentSKeyCount, 0);
        ASSERT_EQ(kkvDocs.size(), 0);
        ASSERT_EQ(searchContext.currentSKeyCount, 0);
        ASSERT_EQ(searchContext.seekSKeyCount, 1);
        ASSERT_EQ(searchContext.seekRtSegmentCount, 0);
        ASSERT_EQ(searchContext.seekSegmentCount, 0);
    }

    {
        PKeyType pkey = 4;
        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);

        KKVDocs kkvDocs(_pool.get());
        auto status = KKVSearchCoroutine<SKeyType>::SearchBuilding(searchContext, kkvDocs);
        ASSERT_TRUE(status.IsOK());
        ASSERT_FALSE(searchContext.hasPKeyDeleted);
        ASSERT_EQ(searchContext.currentSKeyCount, 0);
        ASSERT_EQ(kkvDocs.size(), 0);
        ASSERT_EQ(searchContext.currentSKeyCount, 0);
        ASSERT_EQ(searchContext.seekSKeyCount, 0);
        ASSERT_EQ(searchContext.seekRtSegmentCount, 0);
        ASSERT_EQ(searchContext.seekSegmentCount, 0);

        status = future_lite::interface::syncAwait(KKVSearchCoroutine<SKeyType>::SearchBuilt(searchContext, kkvDocs),
                                                   &_executor);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(searchContext.hasPKeyDeleted);
        ASSERT_EQ(kkvDocs.size(), 0);
        ASSERT_EQ(searchContext.currentSKeyCount, 0);
        ASSERT_EQ(searchContext.seekSKeyCount, 1);
        ASSERT_EQ(searchContext.seekRtSegmentCount, 1);
        ASSERT_EQ(searchContext.seekSegmentCount, 1);
    }
}

TEST_F(KKVSearchCoroutineTest, TestSKeyCountLimits)
{
    PrepareSchema();
    PrepareReaders();

    PKeyType pkey = 1;
    int currentTsInSecond = 0;
    bool keepSortSeq = false;
    int skeyCountLimits = 1;
    KKVMetricsCollector* metricsCollector = nullptr;

    {
        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);
        KKVDocs kkvDocs(_pool.get());
        auto status = KKVSearchCoroutine<SKeyType>::SearchBuilding(searchContext, kkvDocs);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvDocs.size(), 1);
        ASSERT_EQ(searchContext.currentSKeyCount, 1);
        ASSERT_EQ(searchContext.seekSKeyCount, 1);
    }

    {
        skeyCountLimits = 4;
        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);
        KKVDocs kkvDocs(_pool.get());
        auto status = KKVSearchCoroutine<SKeyType>::SearchBuilding(searchContext, kkvDocs);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvDocs.size(), 1);
        ASSERT_EQ(searchContext.currentSKeyCount, 1);
        ASSERT_EQ(searchContext.seekSKeyCount, 1);
    }

    {
        skeyCountLimits = 1;
        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);
        KKVDocs kkvDocs(_pool.get());
        auto status = future_lite::interface::syncAwait(
            KKVSearchCoroutine<SKeyType>::SearchBuilt(searchContext, kkvDocs), &_executor);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvDocs.size(), 1);
        ASSERT_EQ(searchContext.currentSKeyCount, 1);
        // coroutine will search two iter with single thread concurrency
        ASSERT_EQ(searchContext.seekSKeyCount, 3);
    }

    {
        skeyCountLimits = 2;
        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);
        KKVDocs kkvDocs(_pool.get());
        auto status = future_lite::interface::syncAwait(
            KKVSearchCoroutine<SKeyType>::SearchBuilt(searchContext, kkvDocs), &_executor);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(kkvDocs.size(), 2);
        ASSERT_EQ(searchContext.currentSKeyCount, 2);
        // coroutine will search two iter with single thread concurrency
        ASSERT_EQ(searchContext.seekSKeyCount, 3);
    }
}

TEST_F(KKVSearchCoroutineTest, TestMinLocator)
{
    PrepareSchema();
    PrepareReaders();

    Locator minLocator;
    minLocator.SetMultiProgress({{GetProgress(0, 32767, 100), GetProgress(32768, 65535, 102)}});

    PKeyType pkey = 1;
    int currentTsInSecond = 0;
    bool keepSortSeq = false;
    int skeyCountLimits = 1024;
    KKVMetricsCollector* metricsCollector = nullptr;

    {
        // rt segment valid, LCR_FULLY_FASTER
        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);
        searchContext.minLocator = &minLocator;

        KKVDocs kkvDocs(_pool.get());
        auto status = future_lite::interface::syncAwait(
            KKVSearchCoroutine<SKeyType>::SearchBuilt(searchContext, kkvDocs), &_executor);
        ASSERT_TRUE(status.IsOK());

        ASSERT_EQ(kkvDocs.size(), 1);
        ASSERT_EQ(kkvDocs[0].skey, 2);
        ASSERT_EQ(kkvDocs[0].timestamp, 500);

        ASSERT_EQ(searchContext.currentSKeyCount, 1);
        ASSERT_EQ(searchContext.seekSKeyCount, 1);
        ASSERT_EQ(searchContext.seekRtSegmentCount, 1);
        ASSERT_EQ(searchContext.seekSegmentCount, 1);
        ASSERT_EQ(searchContext.builtFoundSKeys.size(), 1);
    }

    {
        // rt segment valid, LCR_PARTIAL_FASTER
        minLocator.SetMultiProgress({{GetProgress(0, 32767, 100), GetProgress(32768, 65535, 104)}});
        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);
        searchContext.minLocator = &minLocator;

        KKVDocs kkvDocs(_pool.get());
        auto status = future_lite::interface::syncAwait(
            KKVSearchCoroutine<SKeyType>::SearchBuilt(searchContext, kkvDocs), &_executor);
        ASSERT_TRUE(status.IsOK());

        ASSERT_EQ(kkvDocs.size(), 1);
        ASSERT_EQ(kkvDocs[0].skey, 2);
        ASSERT_EQ(kkvDocs[0].timestamp, 500);

        ASSERT_EQ(searchContext.currentSKeyCount, 1);
        ASSERT_EQ(searchContext.seekSKeyCount, 1);
        ASSERT_EQ(searchContext.seekRtSegmentCount, 1);
        ASSERT_EQ(searchContext.seekSegmentCount, 1);
        ASSERT_EQ(searchContext.builtFoundSKeys.size(), 1);
    }

    {
        // all segment valid
        minLocator.SetMultiProgress({{GetProgress(0, 32767, 96), GetProgress(32768, 65535, 98)}});

        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);
        searchContext.minLocator = &minLocator;

        KKVDocs kkvDocs(_pool.get());
        auto status = future_lite::interface::syncAwait(
            KKVSearchCoroutine<SKeyType>::SearchBuilt(searchContext, kkvDocs), &_executor);
        ASSERT_TRUE(status.IsOK());

        ASSERT_EQ(kkvDocs.size(), 2);
        ASSERT_EQ(kkvDocs[0].skey, 2);
        ASSERT_EQ(kkvDocs[0].timestamp, 500);
        ASSERT_EQ(kkvDocs[1].skey, 1);
        ASSERT_EQ(kkvDocs[1].timestamp, 100);

        ASSERT_EQ(searchContext.currentSKeyCount, 2);
        ASSERT_EQ(searchContext.seekSKeyCount, 3);
        ASSERT_EQ(searchContext.seekRtSegmentCount, 1);
        ASSERT_EQ(searchContext.seekSegmentCount, 2);
        ASSERT_EQ(searchContext.builtFoundSKeys.size(), 2);
    }

    {
        // no segment valid
        minLocator.SetMultiProgress({{GetProgress(0, 32767, 106), GetProgress(32768, 65535, 108)}});

        SearchContext searchContext(_pool.get(), _indexConfig.get(), pkey, _buildingSegReaders, _builtSegReaders,
                                    currentTsInSecond, keepSortSeq, skeyCountLimits, metricsCollector);
        searchContext.minLocator = &minLocator;

        KKVDocs kkvDocs(_pool.get());
        auto status = future_lite::interface::syncAwait(
            KKVSearchCoroutine<SKeyType>::SearchBuilt(searchContext, kkvDocs), &_executor);
        ASSERT_TRUE(status.IsOK());

        ASSERT_EQ(kkvDocs.size(), 0);
        ASSERT_EQ(searchContext.currentSKeyCount, 0);
        ASSERT_EQ(searchContext.seekSKeyCount, 0);
        ASSERT_EQ(searchContext.seekRtSegmentCount, 0);
        ASSERT_EQ(searchContext.seekSegmentCount, 0);
        ASSERT_EQ(searchContext.builtFoundSKeys.size(), 0);
    }
}

} // namespace indexlibv2::index
