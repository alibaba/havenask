#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
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
using namespace indexlib::document;

namespace indexlibv2::index {

class NormalKKVBuiltSegmentIteratorTest : public TESTBASE
{
public:
    void setUp() override;
    void PrepareSchema();
    void PrepareIndexData(const std::string& docStr);

private:
    void CheckResultBuffer(const std::vector<uint64_t>& expectedSKeys, const std::vector<int64_t>& expectedValues,
                           const std::vector<uint32_t>& expectedTs, KKVResultBuffer& actualResultBuffer);

private:
    std::shared_ptr<config::TabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<index::PlainFormatEncoder> _plainFormatEncoder;
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    std::shared_ptr<indexlib::file_system::IDirectory> _indexDirectory;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, NormalKKVBuiltSegmentIteratorTest);

TEST_F(NormalKKVBuiltSegmentIteratorTest, TestSimpleProcess)
{
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=12,value=2,ts=200000000;"
                    "cmd=add,pkey=2,skey=2,value=1,ts=300000000;"
                    "cmd=add,pkey=1,skey=13,value=3,ts=300000000;";
    PrepareIndexData(docStr);
    // Load by KKVBuiltSegmentReader
    using SKeyType = int32_t;
    KKVBuiltSegmentReader<SKeyType> reader(100, true);
    {
        auto status = reader.Open(_indexConfig, _indexDirectory);
        ASSERT_TRUE(status.IsOK()) << status.ToString();
    }

    auto metricsCollector = std::make_shared<index::KVMetricsCollector>();
    auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), metricsCollector.get()));
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(iter);
    // check value
    std::vector<uint64_t> expectedSKeys = {1, 12, 13};
    std::vector<int64_t> expectedValues = {1, 2, 3};
    std::vector<uint32_t> expectedTs = {100, 200, 300};
    int cursor = 0;
    while (iter->IsValid()) {
        auto skey = iter->GetCurrentSkey();
        autil::StringView value;
        iter->GetCurrentValue(value);
        auto timestamp = iter->GetCurrentTs();
        ASSERT_EQ(skey, expectedSKeys[cursor]);
        ASSERT_EQ(*((int64_t*)value.data()), expectedValues[cursor]);
        ASSERT_EQ(timestamp, expectedTs[cursor]);
        iter->MoveToNext();
        ++cursor;
    }
}

TEST_F(NormalKKVBuiltSegmentIteratorTest, TestDuplicatedSKey)
{
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=2,value=2,ts=200000000;"
                    "cmd=add,pkey=2,skey=1,value=1,ts=300000000;"
                    "cmd=add,pkey=1,skey=1,value=12,ts=400000000;"
                    "cmd=add,pkey=1,skey=2,value=22,ts=500000000;"
                    "cmd=add,pkey=1,skey=3,value=3,ts=600000000;";
    PrepareIndexData(docStr);
    // Load by KKVBuiltSegmentReader
    using SKeyType = int32_t;
    KKVBuiltSegmentReader<SKeyType> reader(100, true);
    {
        auto status = reader.Open(_indexConfig, _indexDirectory);
        ASSERT_TRUE(status.IsOK()) << status.ToString();
    }
    auto metricsCollector = std::make_shared<index::KVMetricsCollector>();
    auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), metricsCollector.get()));
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(iter);
    // check value
    std::vector<uint64_t> expectedSKeys = {1, 2, 3};
    std::vector<int64_t> expectedValues = {12, 22, 3};
    std::vector<uint32_t> expectedTs = {400, 500, 600};
    int cursor = 0;
    while (iter->IsValid()) {
        auto skey = iter->GetCurrentSkey();
        autil::StringView value;
        iter->GetCurrentValue(value);
        auto timestamp = iter->GetCurrentTs();
        ASSERT_EQ(skey, expectedSKeys[cursor]);
        ASSERT_EQ(*((int64_t*)value.data()), expectedValues[cursor]);
        ASSERT_EQ(timestamp, expectedTs[cursor]);
        iter->MoveToNext();
        ++cursor;
    }
}

TEST_F(NormalKKVBuiltSegmentIteratorTest, TestSkeyDeleted)
{
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=12,value=12,ts=200000000;"
                    "cmd=add,pkey=1,skey=13,value=13,ts=300000000;"
                    "cmd=delete,pkey=1,skey=12,ts=400000000;"
                    "cmd=add,pkey=1,skey=14,value=14,ts=500000000;"
                    "cmd=delete,pkey=1,skey=13,ts=600000000;"
                    "cmd=add,pkey=1,skey=13,value=131,ts=700000000;";
    PrepareIndexData(docStr);
    // Load by KKVBuiltSegmentReader
    using SKeyType = int32_t;
    KKVBuiltSegmentReader<SKeyType> reader(100, true);
    {
        auto status = reader.Open(_indexConfig, _indexDirectory);
        ASSERT_TRUE(status.IsOK()) << status.ToString();
    }

    {
        auto metricsCollector = std::make_shared<index::KVMetricsCollector>();
        auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), metricsCollector.get()));
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(iter);

        // check all value
        std::vector<uint64_t> expectedSKeyDeletedFlags = {false, true, false, false};
        std::vector<uint64_t> expectedSKeys = {1, 12, 13, 14};
        std::vector<int64_t> expectedValues = {1, -1, 131, 14};
        std::vector<uint32_t> expectedTs = {100, 400, 700, 500};
        int cursor = 0;
        while (iter->IsValid()) {
            auto skey = iter->GetCurrentSkey();
            bool skeyDeleted = iter->CurrentSkeyDeleted();

            auto timestamp = iter->GetCurrentTs();
            ASSERT_EQ(skey, expectedSKeys[cursor]);
            ASSERT_EQ(timestamp, expectedTs[cursor]);
            ASSERT_EQ(skeyDeleted, expectedSKeyDeletedFlags[cursor]);
            if (!skeyDeleted) {
                autil::StringView value;
                iter->GetCurrentValue(value);
                ASSERT_EQ(*((int64_t*)value.data()), expectedValues[cursor]);
            }
            iter->MoveToNext();
            ++cursor;
        }
    }
}

TEST_F(NormalKKVBuiltSegmentIteratorTest, TestExpireTime)
{
    using SKeyType = int32_t;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;

    auto schemaImpl = _schema->TEST_GetImpl();
    schemaImpl->SetRuntimeSetting("ttl_field_name", "ttl", true);
    schemaImpl->SetRuntimeSetting("enable_ttl", true, true);
    schemaImpl->SetRuntimeSetting("ttl_from_doc", true, true);
    ASSERT_TRUE(
        framework::TabletSchemaLoader::ResolveSchema(/*options*/ nullptr, /*indexPath*/ "", _schema.get()).IsOK());

    _indexConfig->SetTTL(20000);
    _indexConfig->EnableStoreExpireTime();
    ASSERT_TRUE(_indexConfig->StoreExpireTime());
    ASSERT_TRUE(_indexConfig->TTLEnabled());
    // currentTs = 401;
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ttl=500,ts=100000000;"
                    "cmd=add,pkey=1,skey=2,value=2,ttl=100,ts=200000000;" // expire
                    "cmd=add,pkey=1,skey=3,value=3,ttl=50,ts=350000000;"  // expire
                    "cmd=add,pkey=1,skey=4,value=4,ts=400000000;"         // use default ttl
                    "cmd=add,pkey=1,skey=5,value=5,ttl=100,ts=500000000;"
                    "cmd=add,pkey=1,skey=6,value=6,ttl=300,ts=100000000;"; // expire
    PrepareIndexData(docStr);
    // Load by KKVBuiltSegmentReader
    KKVBuiltSegmentReader<SKeyType> reader(100, true);
    {
        auto status = reader.Open(_indexConfig, _indexDirectory);
        ASSERT_TRUE(status.IsOK()) << status.ToString();
    }

    auto metricsCollector = std::make_shared<index::KVMetricsCollector>();
    auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), metricsCollector.get()));
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(iter);

    SKeyContext* skeyContext = nullptr;
    uint64_t minimumTsInSecond = 0;
    uint64_t currentTsInSecond = 401;
    PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(_pool.get())};
    KKVResultBuffer resultBuffer {1024 * 1024 * 4, 100, _pool.get()};
    iter->BatchGet(skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
    ASSERT_EQ(resultBuffer.Size(), 3);
    ASSERT_EQ(foundSkeys.size(), 6);

    std::vector<uint64_t> expectedSKeys = {1, 4, 5};
    std::vector<int64_t> expectedValues = {1, 4, 5};
    std::vector<uint32_t> expectedTs = {100, 400, 500};
    int cursor = 0;
    while (resultBuffer.IsValid()) {
        ASSERT_EQ(resultBuffer.GetCurrentSkey(), expectedSKeys[cursor]);
        ASSERT_EQ(resultBuffer.GetCurrentTimestamp(), expectedTs[cursor]);
        autil::StringView value;
        resultBuffer.GetCurrentValue(value);
        ASSERT_EQ(*((int64_t*)value.data()), expectedValues[cursor]);
        resultBuffer.MoveToNext();
        ++cursor;
    }
}

TEST_F(NormalKKVBuiltSegmentIteratorTest, TestFoundKeys)
{
    using SKeyType = int32_t;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=12,value=12,ts=200000000;"
                    "cmd=add,pkey=1,skey=13,value=13,ts=300000000;"
                    "cmd=delete,pkey=1,skey=12,ts=400000000;"
                    "cmd=add,pkey=1,skey=14,value=14,ts=500000000;"
                    "cmd=delete,pkey=1,skey=13,ts=600000000;"
                    "cmd=add,pkey=1,skey=13,value=131,ts=700000000;";
    PrepareIndexData(docStr);
    // Load by KKVBuiltSegmentReader
    KKVBuiltSegmentReader<SKeyType> reader(100, true);
    {
        auto status = reader.Open(_indexConfig, _indexDirectory);
        ASSERT_TRUE(status.IsOK()) << status.ToString();
    }

    auto metricsCollector = std::make_shared<index::KVMetricsCollector>();
    auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), metricsCollector.get()));
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(iter);

    SKeyContext* skeyContext = nullptr;
    uint64_t minimumTsInSecond = 0;
    uint64_t currentTsInSecond = 0;
    PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(_pool.get())};
    KKVResultBuffer resultBuffer {1024 * 1024 * 4, 100, _pool.get()};
    iter->BatchGet(skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
    ASSERT_EQ(resultBuffer.Size(), 3);
    // 1 deleted skey + 3 valid skey
    ASSERT_EQ(foundSkeys.size(), 4);
}

TEST_F(NormalKKVBuiltSegmentIteratorTest, TestFillBuffer)
{
    using SKeyType = int32_t;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=12,value=12,ts=200000000;"
                    "cmd=add,pkey=1,skey=13,value=13,ts=300000000;"
                    "cmd=delete,pkey=1,skey=12,ts=400000000;"
                    "cmd=add,pkey=1,skey=14,value=14,ts=500000000;"
                    "cmd=delete,pkey=1,skey=13,ts=600000000;"
                    "cmd=add,pkey=1,skey=13,value=131,ts=700000000;";
    PrepareIndexData(docStr);
    // Load by KKVBuiltSegmentReader
    KKVBuiltSegmentReader<SKeyType> reader(100, true);
    {
        auto status = reader.Open(_indexConfig, _indexDirectory);
        ASSERT_TRUE(status.IsOK()) << status.ToString();
    }

    auto metricsCollector = std::make_shared<index::KVMetricsCollector>();
    auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), metricsCollector.get()));
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(iter);

    SKeyContext* skeyContext = nullptr;
    uint64_t minimumTsInSecond = 0;
    uint64_t currentTsInSecond = 0;
    PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(_pool.get())};
    KKVResultBuffer resultBuffer {1024 * 1024 * 4, 100, _pool.get()};
    iter->BatchGet(skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
    ASSERT_EQ(resultBuffer.Size(), 3);

    std::vector<uint64_t> expectedSKeys = {1, 13, 14};
    std::vector<int64_t> expectedValues = {1, 131, 14};
    std::vector<uint32_t> expectedTs = {100, 700, 500};
    int cursor = 0;
    while (resultBuffer.IsValid()) {
        ASSERT_EQ(resultBuffer.GetCurrentSkey(), expectedSKeys[cursor]);
        ASSERT_EQ(resultBuffer.GetCurrentTimestamp(), expectedTs[cursor]);
        autil::StringView value;
        resultBuffer.GetCurrentValue(value);
        ASSERT_EQ(*((int64_t*)value.data()), expectedValues[cursor]);
        resultBuffer.MoveToNext();
        ++cursor;
    }
}

TEST_F(NormalKKVBuiltSegmentIteratorTest, TestBatchGet)
{
    using SKeyType = int32_t;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=12,value=2,ts=200000000;"
                    "cmd=add,pkey=2,skey=2,value=1,ts=300000000;"
                    "cmd=add,pkey=1,skey=13,value=3,ts=300000000;";
    PrepareIndexData(docStr);

    KKVBuiltSegmentReader<SKeyType> reader(100, true);
    auto status = reader.Open(_indexConfig, _indexDirectory);
    ASSERT_TRUE(status.IsOK()) << status.ToString();

    // Test reach limit
    {
        auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), nullptr));
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(iter);

        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(_pool.get())};
        KKVResultBuffer resultBuffer {64, 2, _pool.get()};
        iter->BatchGet(nullptr, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        ASSERT_EQ(resultBuffer.Size(), 2);
        std::vector<uint64_t> expectedSKeys = {1, 12};
        std::vector<int64_t> expectedValues = {1, 2};
        std::vector<uint32_t> expectedTs = {100, 200};
        CheckResultBuffer(expectedSKeys, expectedValues, expectedTs, resultBuffer);
        ASSERT_EQ(foundSkeys.size(), 2);
        ASSERT_NE(foundSkeys.find(1), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(12), foundSkeys.end());
        ASSERT_FALSE(iter->IsValid());
    }
    // Test buffer full
    {
        auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), nullptr));
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(iter);

        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(_pool.get())};
        KKVResultBuffer resultBuffer {2, 1024, _pool.get()};
        iter->BatchGet(nullptr, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        ASSERT_EQ(resultBuffer.Size(), 2);
        std::vector<uint64_t> expectedSKeys = {1, 12};
        std::vector<int64_t> expectedValues = {1, 2};
        std::vector<uint32_t> expectedTs = {100, 200};
        CheckResultBuffer(expectedSKeys, expectedValues, expectedTs, resultBuffer);
        ASSERT_EQ(foundSkeys.size(), 2);
        ASSERT_NE(foundSkeys.find(1), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(12), foundSkeys.end());
        ASSERT_TRUE(iter->IsValid());
        ASSERT_EQ(13, iter->GetCurrentSkey());
        ASSERT_EQ(300, iter->GetCurrentTs());
        ASSERT_EQ(0, iter->GetCurrentExpireTime());
    }
    // Test normal finish
    {
        auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), nullptr));
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(iter);

        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(_pool.get())};
        KKVResultBuffer resultBuffer {1024, 1024, _pool.get()};
        iter->BatchGet(nullptr, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        ASSERT_EQ(resultBuffer.Size(), 3);
        std::vector<uint64_t> expectedSKeys = {1, 12, 13};
        std::vector<int64_t> expectedValues = {1, 2, 3};
        std::vector<uint32_t> expectedTs = {100, 200, 300};
        CheckResultBuffer(expectedSKeys, expectedValues, expectedTs, resultBuffer);
        ASSERT_EQ(foundSkeys.size(), 3);
        ASSERT_NE(foundSkeys.find(1), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(12), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(13), foundSkeys.end());
        ASSERT_FALSE(iter->IsValid());
    }
    // Test required skey
    {
        auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), nullptr));
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(iter);

        SKeySearchContext<SKeyType> skeyContext(_pool.get());
        // 14 > 13
        skeyContext.Init(vector<uint64_t> {(uint64_t)14});
        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(_pool.get())};
        KKVResultBuffer resultBuffer {1024, 1024, _pool.get()};
        iter->BatchGet(&skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        ASSERT_EQ(resultBuffer.Size(), 0);
        ASSERT_EQ(foundSkeys.size(), 0);
        ASSERT_FALSE(iter->IsValid());
    }
}

void NormalKKVBuiltSegmentIteratorTest::setUp()
{
    _pool.reset(new Pool());

    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::IDirectory::Get(fs);
    _indexDirectory = _rootDir->MakeDirectory("index", indexlib::file_system::DirectoryOption::Mem()).GetOrThrow();

    PrepareSchema();

    const auto& valueConfig = _indexConfig->GetValueConfig();
    if (!valueConfig->IsSimpleValue()) {
        auto [status, attributeConfig] = valueConfig->CreatePackAttributeConfig();
        ASSERT_TRUE(status.IsOK());
        _plainFormatEncoder.reset(index::PackAttributeFormatter::CreatePlainFormatEncoder(attributeConfig));
    }
}

void NormalKKVBuiltSegmentIteratorTest::PrepareSchema()
{
    string schemaJsonStr = R"({
        "attributes" : [ "value" ],
        "fields" : [
            {"field_type" : "UINT64", "field_name" : "pkey"},
            {"field_type" : "UINT64", "field_name" : "skey"},
            {"field_type" : "INT64", "field_name" : "value"}
        ],
        "indexs" : [ {
            "index_name" : "pkey_skey",
            "use_number_hash": true,
            "index_type" : "PRIMARY_KEY",
            "index_fields" : [
                {"field_name" : "pkey", "key_type" : "prefix"},
                {"field_name" : "skey", "key_type" : "suffix"}
            ],
            "value_fields" : [
                {"field_type" : "INT64", "field_name" : "value"}
            ]
        } ],
        "table_name" : "kkv_table",
        "table_type" : "kkv",
        "enable_ttl": true
    })";

    _schema.reset(framework::TabletSchemaLoader::LoadSchema(schemaJsonStr).release());
    ASSERT_TRUE(_schema);
    auto kkvIndexConfigs = _schema->GetIndexConfigs();
    ASSERT_EQ(1, kkvIndexConfigs.size());

    _indexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(kkvIndexConfigs[0]);
    ASSERT_NE(nullptr, _indexConfig);
    ASSERT_TRUE(_indexConfig->UseNumberHash());

    _indexConfig->GetIndexPreference().GetHashDictParam().SetHashType(KKV_PKEY_DENSE_TABLE_NAME);
}

void NormalKKVBuiltSegmentIteratorTest::PrepareIndexData(const std::string& docStr)
{
    KKVMemIndexer<int32_t> memIndexer("test", 1024, 1024, 1024, 1.0, 1.0, true);
    ASSERT_TRUE(memIndexer.Init(_indexConfig, nullptr).IsOK());

    auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
    // Build
    for (const auto& rawDoc : rawDocs) {
        auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
        ASSERT_TRUE(docBatch);
        auto s = memIndexer.Build(docBatch.get());
        ASSERT_TRUE(s.IsOK()) << s.ToString();
    }
    // Dump
    auto dumpPool = std::make_shared<autil::mem_pool::UnsafePool>(1024 * 1024);
    std::shared_ptr<framework::DumpParams> dumpParams;
    auto status = memIndexer.Dump(dumpPool.get(), indexlib::file_system::IDirectory::ToLegacyDirectory(_indexDirectory),
                                  dumpParams);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_TRUE(_indexDirectory->IsExist(_indexConfig->GetIndexName()).OK());
    auto result = _indexDirectory->GetDirectory(_indexConfig->GetIndexName());
    ASSERT_TRUE(result.OK());
    auto kkvDir = result.Value();
    ASSERT_TRUE(kkvDir);
    ASSERT_TRUE(kkvDir->IsExist("pkey").GetOrThrow());
    ASSERT_TRUE(kkvDir->IsExist("skey").GetOrThrow());
    ASSERT_TRUE(kkvDir->IsExist("value").GetOrThrow());
}

void NormalKKVBuiltSegmentIteratorTest::CheckResultBuffer(const std::vector<uint64_t>& expectedSKeys,
                                                          const std::vector<int64_t>& expectedValues,
                                                          const std::vector<uint32_t>& expectedTs,
                                                          KKVResultBuffer& resultBuffer)
{
    int cursor = 0;
    while (resultBuffer.IsValid()) {
        ASSERT_EQ(resultBuffer.GetCurrentSkey(), expectedSKeys[cursor]);
        ASSERT_EQ(resultBuffer.GetCurrentTimestamp(), expectedTs[cursor]);
        autil::StringView value;
        resultBuffer.GetCurrentValue(value);
        ASSERT_EQ(*((int64_t*)value.data()), expectedValues[cursor]);
        resultBuffer.MoveToNext();
        ++cursor;
    }
}

} // namespace indexlibv2::index
