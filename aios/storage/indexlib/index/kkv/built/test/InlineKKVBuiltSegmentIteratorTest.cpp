#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/kkv/building/KKVMemIndexer.h"
#include "indexlib/index/kkv/built/KKVBuiltSegmentReader.h"
#include "indexlib/index/kkv/common/KKVIndexFormat.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/dump/KKVDataDumperFactory.h"
#include "indexlib/table/kkv_table/KKVSchemaResolver.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlibv2::index {

class InlineKKVBuiltSegmentIteratorTest : public TESTBASE
{
public:
    void setUp() override;
    void PrepareSchema();

    template <typename SKeyType>
    void PrepareIndexData(const std::map<uint64_t, std::vector<KKVDoc>>& kkvDocsMap);

private:
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<indexlib::file_system::IDirectory> _indexDirectory;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, InlineKKVBuiltSegmentIteratorTest);

void InlineKKVBuiltSegmentIteratorTest::setUp()
{
    _pool.reset(new Pool());
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::IDirectory::Get(fs);
    _indexDirectory = rootDir->MakeDirectory("index", indexlib::file_system::DirectoryOption::Mem()).GetOrThrow();
    PrepareSchema();
}

void InlineKKVBuiltSegmentIteratorTest::PrepareSchema()
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
            ],
            "index_preference":{"value_inline": true}
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
    _indexConfig->GetIndexPreference().GetHashDictParam().SetHashType(KKV_PKEY_DENSE_TABLE_NAME);
}

template <typename SKeyType>
void InlineKKVBuiltSegmentIteratorTest::PrepareIndexData(const std::map<uint64_t, std::vector<KKVDoc>>& kkvDocsMap)
{
    auto dumpDirectory =
        _indexDirectory->MakeDirectory("pkey_skey", indexlib::file_system::DirectoryOption::Mem()).GetOrThrow();

    auto dumper = KKVDataDumperFactory::Create<SKeyType>(_indexConfig, true, KKVDumpPhrase::MERGE_BOTTOMLEVEL);
    ASSERT_NE(nullptr, dumper);
    auto writerOption = indexlib::file_system::WriterOption::Buffer();
    ASSERT_TRUE(dumper->Init(dumpDirectory, writerOption, writerOption, kkvDocsMap.size()).IsOK());

    for (const auto& [pk, kkvDocs] : kkvDocsMap) {
        for (size_t i = 0; i < kkvDocs.size(); ++i) {
            ASSERT_TRUE(dumper->Dump(pk, false, i + 1 == kkvDocs.size(), kkvDocs[i]).IsOK());
        }
    }
    ASSERT_TRUE(dumper->Close().IsOK());
    //     KKVIndexFormat(bool storeTs, bool keepSortSeq, bool valueInline);
    KKVIndexFormat indexFormat(true, false, true);
    ASSERT_TRUE(indexFormat.Store(dumpDirectory).IsOK());
}

TEST_F(InlineKKVBuiltSegmentIteratorTest, TestSimpleProcess)
{
    std::vector<uint64_t> pks = {1, 1, 2, 1};
    std::vector<uint64_t> skeys = {1, 12, 2, 13};
    std::vector<int64_t> values = {1, 2, 1, 3};
    std::vector<uint32_t> timestamps = {100, 200, 300, 300};
    std::map<uint64_t, std::vector<KKVDoc>> kkvDocsMap;
    for (size_t i = 0; i < pks.size(); ++i) {
        kkvDocsMap[pks[i]].emplace_back(
            skeys[i], timestamps[i], autil::StringView(reinterpret_cast<const char*>(&values[i]), sizeof(values[i])));
    }
    PrepareIndexData<int32_t>(kkvDocsMap);

    KKVBuiltSegmentReader<int32_t> reader(100, true);
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
        ASSERT_EQ(skey, expectedSKeys[cursor]) << cursor;
        auto timestamp = iter->GetCurrentTs();
        ASSERT_EQ(timestamp, expectedTs[cursor]) << cursor;
        autil::StringView value;
        iter->GetCurrentValue(value);
        int64_t int64Val = 0;
        memcpy(&int64Val, value.data(), sizeof(int64Val));
        ASSERT_EQ(int64Val, expectedValues[cursor]) << cursor;
        iter->MoveToNext();
        ++cursor;
    }
}

TEST_F(InlineKKVBuiltSegmentIteratorTest, TestSkeyDeleted)
{
    using SKeyType = int32_t;
    std::vector<uint64_t> pks = {1, 1, 1, 1};
    std::vector<uint64_t> skeys = {1, 12, 13, 14};
    std::vector<int64_t> values = {1, -1, 131, 14};
    std::vector<uint32_t> timestamps = {100, 400, 700, 500};
    std::map<uint64_t, std::vector<KKVDoc>> kkvDocsMap;
    for (size_t i = 0; i < pks.size(); ++i) {
        if (values[i] == -1) {
            kkvDocsMap[pks[i]].emplace_back(skeys[i], timestamps[i]);
            kkvDocsMap[pks[i]].back().skeyDeleted = true;
            continue;
        }
        kkvDocsMap[pks[i]].emplace_back(
            skeys[i], timestamps[i], autil::StringView(reinterpret_cast<const char*>(&values[i]), sizeof(values[i])));
    }
    PrepareIndexData<SKeyType>(kkvDocsMap);
    KKVBuiltSegmentReader<SKeyType> reader(100, true);
    {
        auto status = reader.Open(_indexConfig, _indexDirectory);
        ASSERT_TRUE(status.IsOK()) << status.ToString();
    }

    {
        auto [status, iter] = future_lite::interface::syncAwait(reader.Lookup(1, _pool.get(), nullptr));
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
                int64_t int64Val = 0;
                memcpy(&int64Val, value.data(), sizeof(int64Val));
                ASSERT_EQ(int64Val, expectedValues[cursor]) << cursor;
            }
            iter->MoveToNext();
            ++cursor;
        }
    }
}

TEST_F(InlineKKVBuiltSegmentIteratorTest, TestExpireTime)
{
    using SKeyType = int32_t;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;
    _indexConfig->SetTTL(20000);
    _indexConfig->EnableStoreExpireTime();
    ASSERT_TRUE(_indexConfig->StoreExpireTime());
    ASSERT_TRUE(_indexConfig->TTLEnabled());
    // currentTs = 401;
    std::vector<uint64_t> pks = {1, 1, 1, 1, 1, 1};
    std::vector<uint64_t> skeys = {1, 2, 3, 4, 5, 6};
    std::vector<int64_t> values = {1, 2, 3, 4, 5, 6};
    std::vector<uint32_t> ttls = {500, 100, 50, 20000, 100, 300};
    std::vector<uint32_t> timestamps = {100, 200, 350, 400, 500, 100};
    std::map<uint64_t, std::vector<KKVDoc>> kkvDocsMap;
    for (size_t i = 0; i < pks.size(); ++i) {
        kkvDocsMap[pks[i]].emplace_back(
            skeys[i], timestamps[i], autil::StringView(reinterpret_cast<const char*>(&values[i]), sizeof(values[i])));
        kkvDocsMap[pks[i]].back().expireTime = ttls[i] + timestamps[i];
    }
    PrepareIndexData<SKeyType>(kkvDocsMap);

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

TEST_F(InlineKKVBuiltSegmentIteratorTest, TestFoundKeys)
{
    using SKeyType = int32_t;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;
    std::vector<uint64_t> pks = {1, 1, 1, 1};
    std::vector<uint64_t> skeys = {1, 12, 13, 14};
    std::vector<int64_t> values = {1, -1, 131, 14};
    std::vector<uint32_t> timestamps = {100, 400, 700, 500};
    std::map<uint64_t, std::vector<KKVDoc>> kkvDocsMap;
    for (size_t i = 0; i < pks.size(); ++i) {
        if (values[i] == -1) {
            kkvDocsMap[pks[i]].emplace_back(skeys[i], timestamps[i]);
            kkvDocsMap[pks[i]].back().skeyDeleted = true;
            continue;
        }
        kkvDocsMap[pks[i]].emplace_back(
            skeys[i], timestamps[i], autil::StringView(reinterpret_cast<const char*>(&values[i]), sizeof(values[i])));
    }
    PrepareIndexData<SKeyType>(kkvDocsMap);

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
    ASSERT_EQ(foundSkeys.size(), 4);
}

TEST_F(InlineKKVBuiltSegmentIteratorTest, TestFillBuffer)
{
    using SKeyType = int32_t;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;
    std::vector<uint64_t> pks = {1, 1, 1, 1};
    std::vector<uint64_t> skeys = {1, 12, 13, 14};
    std::vector<int64_t> values = {1, -1, 131, 14};
    std::vector<uint32_t> timestamps = {100, 400, 700, 500};
    std::map<uint64_t, std::vector<KKVDoc>> kkvDocsMap;
    for (size_t i = 0; i < pks.size(); ++i) {
        if (values[i] == -1) {
            kkvDocsMap[pks[i]].emplace_back(skeys[i], timestamps[i]);
            kkvDocsMap[pks[i]].back().skeyDeleted = true;
            continue;
        }
        kkvDocsMap[pks[i]].emplace_back(
            skeys[i], timestamps[i], autil::StringView(reinterpret_cast<const char*>(&values[i]), sizeof(values[i])));
    }
    PrepareIndexData<SKeyType>(kkvDocsMap);

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

} // namespace indexlibv2::index
