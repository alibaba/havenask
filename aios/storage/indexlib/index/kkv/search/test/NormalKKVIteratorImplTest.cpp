
#include "indexlib/index/kkv/search/NormalKKVIteratorImpl.h"

#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/kkv/building/KKVMemIndexer.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentReader.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
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

class NormalKKVIteratorImplTest : public TESTBASE
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
    std::shared_ptr<KKVBuiltSegmentReader<int32_t>>
    CreateBuiltSegmentReader(const std::string& indexDirPath, const std::string& docStr, bool isRtSegment);

private:
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

void NormalKKVIteratorImplTest::PrepareSchema()
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

    //  "store_expire_time": false
    _schema.reset(framework::TabletSchemaLoader::LoadSchema(schemaJsonStr).release());
    auto kkvIndexConfigs = _schema->GetIndexConfigs();
    ASSERT_EQ(1, kkvIndexConfigs.size());

    _indexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(kkvIndexConfigs[0]);
    ASSERT_NE(nullptr, _indexConfig);
    _indexConfig->GetValueConfig()->EnableCompactFormat(true);
    _indexConfig->GetIndexPreference().GetHashDictParam().SetHashType(KKV_PKEY_DENSE_TABLE_NAME);
    _indexConfig->SetUseNumberHash(true);
}

void NormalKKVIteratorImplTest::PrepareReaders()
{
    // offline segment
    auto builtReader1 = CreateBuiltSegmentReader("segment_1", _docStr1, false);
    // dumped rt segment
    auto builtReader2 = CreateBuiltSegmentReader("segment_2", _docStr2, true);
    // building segment
    KKVMemIndexer<SKeyType> memIndexer("test", 1024, 1024, 1024, 1.0, 1.0, true);
    ASSERT_TRUE(memIndexer.Init(_indexConfig, nullptr).IsOK());
    BuildDocs(memIndexer, _docStr3);
    auto buildingReader1 = memIndexer.CreateInMemoryReader();

    auto locator1 = std::make_shared<framework::Locator>();
    locator1->SetProgress({Progress(0, 32767, {97, 0}), Progress(32768, 65535, {99, 0})});
    auto locator2 = std::make_shared<framework::Locator>();
    locator2->SetProgress({Progress(0, 32767, {101, 0}), Progress(32768, 65535, {103, 0})});
    auto locator3 = std::make_shared<framework::Locator>();
    locator3->SetProgress({Progress(0, 32767, {105, 0}), Progress(32768, 65535, {107, 0})});

    _builtSegReaders.emplace_back(std::make_pair(builtReader1, locator1));
    _builtSegReaders.emplace_back(std::make_pair(builtReader2, locator2));
    _buildingSegReaders.emplace_back(std::make_pair(buildingReader1, locator3));
}

void NormalKKVIteratorImplTest::BuildDocs(KKVMemIndexer<int32_t>& memIndexer, const std::string& docStr)
{
    auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
    // Build
    for (const auto& rawDoc : rawDocs) {
        auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
        assert(docBatch);
        auto status = memIndexer.Build(docBatch.get());
        assert(status.IsOK());
    }
}

std::shared_ptr<KKVBuiltSegmentReader<int32_t>>
NormalKKVIteratorImplTest::CreateBuiltSegmentReader(const std::string& indexDirPath, const std::string& docStr,
                                                    bool isRtSegment)
{
    uint32_t segTimestamp = 100;

    KKVMemIndexer<SKeyType> memIndexer("test", 1024, 1024, 1024, 1.0, 1.0, true);
    assert(memIndexer.Init(_indexConfig, nullptr).IsOK());
    BuildDocs(memIndexer, docStr);

    // Dump
    auto dumpPool = std::make_shared<autil::mem_pool::UnsafePool>(1024 * 1024);
    std::shared_ptr<framework::DumpParams> dumpParams;
    auto indexDirectory =
        _rootDir->MakeDirectory(indexDirPath, indexlib::file_system::DirectoryOption::Mem()).GetOrThrow();
    auto status = memIndexer.Dump(dumpPool.get(), indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory),
                                  dumpParams);
    assert(status.IsOK());
    assert(indexDirectory->IsExist(_indexConfig->GetIndexName()).OK());
    auto result = indexDirectory->GetDirectory(_indexConfig->GetIndexName());
    assert(result.OK());
    auto kkvDir = result.Value();
    assert(kkvDir);
    assert(kkvDir->IsExist("pkey").GetOrThrow());
    assert(kkvDir->IsExist("skey").GetOrThrow());
    assert(kkvDir->IsExist("value").GetOrThrow());

    // Load by KKVBuiltSegmentReader
    BuiltSegmentReaderPtr builtReader = std::make_shared<BuiltSegmentReaderTyped>(segTimestamp, isRtSegment);
    assert(builtReader->Open(_indexConfig, indexDirectory).IsOK());

    return builtReader;
}

void NormalKKVIteratorImplTest::setUp()
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
    PrepareSchema();
    PrepareReaders();
}

TEST_F(NormalKKVIteratorImplTest, TestGeneral)
{
    uint64_t ttl = indexlib::DEFAULT_TIME_TO_LIVE;
    PKeyType pkey = 1;
    std::vector<uint64_t> skeyHashs;
    uint64_t curTimeInSecond = 901; // larger than max ts
    NormalKKVIteratorImpl<SKeyType> kkvIterator(_pool.get(), _indexConfig.get(), ttl, pkey, skeyHashs, _builtSegReaders,
                                                _buildingSegReaders, curTimeInSecond);

    KKVResultBuffer resultBuffer(64, std::numeric_limits<uint32_t>::max(), _pool.get());
    kkvIterator.BatchGet(resultBuffer);
    ASSERT_FALSE(kkvIterator.IsValid());
    ASSERT_EQ(2, resultBuffer.Size());

    ASSERT_EQ(2, resultBuffer[0].skey);
    ASSERT_EQ(800, resultBuffer[0].timestamp);
    int16_t expectedVal = 4;
    ASSERT_EQ(autil::StringView((const char*)&expectedVal, sizeof(expectedVal)), resultBuffer[0].value);

    ASSERT_EQ(1, resultBuffer[1].skey);
    ASSERT_EQ(100, resultBuffer[1].timestamp);
    expectedVal = 1;
    ASSERT_EQ(autil::StringView((const char*)&expectedVal, sizeof(expectedVal)), resultBuffer[1].value);
}

TEST_F(NormalKKVIteratorImplTest, TestOnlySegment)
{
    uint64_t ttl = indexlib::DEFAULT_TIME_TO_LIVE;
    PKeyType pkey = 1;
    std::vector<uint64_t> skeyHashs;
    uint64_t curTimeInSecond = 901;
    NormalKKVIteratorImpl<SKeyType> kkvIterator(_pool.get(), _indexConfig.get(), ttl, pkey, skeyHashs,
                                                {_builtSegReaders[0]}, {}, curTimeInSecond);
    KKVResultBuffer resultBuffer(64, std::numeric_limits<uint32_t>::max(), _pool.get());
    kkvIterator.BatchGet(resultBuffer);
    ASSERT_FALSE(kkvIterator.IsValid());
    ASSERT_EQ(2, resultBuffer.Size());

    ASSERT_EQ(1, resultBuffer[0].skey);
    ASSERT_EQ(100, resultBuffer[0].timestamp);
    int16_t expectedVal = 1;
    ASSERT_EQ(autil::StringView((const char*)&expectedVal, sizeof(expectedVal)), resultBuffer[0].value);

    ASSERT_EQ(2, resultBuffer[1].skey);
    ASSERT_EQ(200, resultBuffer[1].timestamp);
    expectedVal = 2;
    ASSERT_EQ(autil::StringView((const char*)&expectedVal, sizeof(expectedVal)), resultBuffer[1].value);
}

TEST_F(NormalKKVIteratorImplTest, TestNotExistedPK)
{
    uint64_t ttl = indexlib::DEFAULT_TIME_TO_LIVE;
    PKeyType pkey = 100;
    std::vector<uint64_t> skeyHashs;
    uint64_t curTimeInSecond = 901;
    NormalKKVIteratorImpl<SKeyType> kkvIterator(_pool.get(), _indexConfig.get(), ttl, pkey, skeyHashs, _builtSegReaders,
                                                _buildingSegReaders, curTimeInSecond);
    KKVResultBuffer resultBuffer(64, std::numeric_limits<uint32_t>::max(), _pool.get());
    kkvIterator.BatchGet(resultBuffer);
    ASSERT_FALSE(kkvIterator.IsValid());
    ASSERT_EQ(0, resultBuffer.Size());
}

TEST_F(NormalKKVIteratorImplTest, TestDeletedPK)
{
    uint64_t ttl = indexlib::DEFAULT_TIME_TO_LIVE;
    PKeyType pkey = 2;
    std::vector<uint64_t> skeyHashs;
    uint64_t curTimeInSecond = 901;
    NormalKKVIteratorImpl<SKeyType> kkvIterator(_pool.get(), _indexConfig.get(), ttl, pkey, skeyHashs, _builtSegReaders,
                                                _buildingSegReaders, curTimeInSecond);
    KKVResultBuffer resultBuffer(64, std::numeric_limits<uint32_t>::max(), _pool.get());
    kkvIterator.BatchGet(resultBuffer);
    ASSERT_FALSE(kkvIterator.IsValid());
    ASSERT_EQ(0, resultBuffer.Size());
}

TEST_F(NormalKKVIteratorImplTest, TestMinTsInSecond)
{
    ASSERT_FALSE(_indexConfig->StoreExpireTime());
    uint64_t ttl = 0; // every skey is expired
    PKeyType pkey = 1;
    std::vector<uint64_t> skeyHashs;
    uint64_t curTimeInSecond = 901;
    // MinTsInSecond = curTimeInSecond - ttl
    NormalKKVIteratorImpl<SKeyType> kkvIterator(_pool.get(), _indexConfig.get(), ttl, pkey, skeyHashs, _builtSegReaders,
                                                _buildingSegReaders, curTimeInSecond);

    KKVResultBuffer resultBuffer(64, std::numeric_limits<uint32_t>::max(), _pool.get());
    kkvIterator.BatchGet(resultBuffer);
    ASSERT_FALSE(kkvIterator.IsValid());
    ASSERT_EQ(0, resultBuffer.Size());
}

TEST_F(NormalKKVIteratorImplTest, TestBufferFull)
{
    ASSERT_FALSE(_indexConfig->StoreExpireTime());
    uint64_t ttl = indexlib::DEFAULT_TIME_TO_LIVE;
    PKeyType pkey = 1;
    std::vector<uint64_t> skeyHashs;
    uint64_t curTimeInSecond = 901;
    NormalKKVIteratorImpl<SKeyType> kkvIterator(_pool.get(), _indexConfig.get(), ttl, pkey, skeyHashs, _builtSegReaders,
                                                _buildingSegReaders, curTimeInSecond);

    {
        KKVResultBuffer resultBuffer(1, std::numeric_limits<uint32_t>::max(), _pool.get());
        kkvIterator.BatchGet(resultBuffer);
        ASSERT_TRUE(kkvIterator.IsValid());
        ASSERT_EQ(1, resultBuffer.Size());
        ASSERT_EQ(2, resultBuffer[0].skey);
    }
    {
        KKVResultBuffer resultBuffer(1, std::numeric_limits<uint32_t>::max(), _pool.get());
        kkvIterator.BatchGet(resultBuffer);
        ASSERT_TRUE(kkvIterator.IsValid());
        ASSERT_EQ(1, resultBuffer.Size());
        ASSERT_EQ(1, resultBuffer[0].skey);
    }
    {
        KKVResultBuffer resultBuffer(1, std::numeric_limits<uint32_t>::max(), _pool.get());
        kkvIterator.BatchGet(resultBuffer);
        ASSERT_FALSE(kkvIterator.IsValid());
    }
}

TEST_F(NormalKKVIteratorImplTest, TestReachLimit)
{
    ASSERT_FALSE(_indexConfig->StoreExpireTime());
    uint64_t ttl = indexlib::DEFAULT_TIME_TO_LIVE;
    PKeyType pkey = 1;
    std::vector<uint64_t> skeyHashs;
    uint64_t curTimeInSecond = 901;
    NormalKKVIteratorImpl<SKeyType> kkvIterator(_pool.get(), _indexConfig.get(), ttl, pkey, skeyHashs, _builtSegReaders,
                                                _buildingSegReaders, curTimeInSecond);

    KKVResultBuffer resultBuffer(64, 1, _pool.get());
    kkvIterator.BatchGet(resultBuffer);
    ASSERT_FALSE(kkvIterator.IsValid());
    ASSERT_EQ(1, resultBuffer.Size());
}

} // namespace indexlibv2::index
