#include "indexlib/index/kkv/building/KKVBuildingSegmentIterator.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTable.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;

namespace indexlibv2::index {

class BuildingKKVSegmentReaderTest : public TESTBASE
{
private:
    template <typename SKeyType>
    void DoTestProcess(FieldType skeyType, SKeyType __for_compile);
    void MakeSchema(FieldType skeyType, int64_t ttl);
    void CheckResultBuffer(const std::vector<uint64_t>& expectedSKeys, const std::vector<uint32_t>& expectedValues,
                           const std::vector<uint32_t>& expectedTs, KKVResultBuffer& actualResultBuffer);

private:
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
};

void BuildingKKVSegmentReaderTest::MakeSchema(FieldType skeyType, int64_t ttl)
{
    string schemaJsonStr = R"(
{
    "index_name": "pkey_skey",
    "index_type": "PRIMARY_KEY",
    "index_fields": [
        {
            "field_name": "pkey",
            "key_type": "prefix"
        },
        {
            "field_name": "skey",
            "key_type": "suffix"
        }
    ],
    "value_fields": [
           "value"
    ],
    "index_preference": {
        "type": "PERF",
        "value_inline": true
    }
}
    )";

    auto indexConfig = std::make_shared<indexlibv2::config::KKVIndexConfig>();
    auto any = autil::legacy::json::ParseJson(schemaJsonStr);
    std::string skeyTypeStr(indexlibv2::config::FieldConfig::FieldTypeToStr(skeyType));
    auto fieldConfigs = config::FieldConfig::TEST_CreateFields("pkey:int32;skey:" + skeyTypeStr + ";value:uint32");
    config::MutableJson runtimeSettings;
    config::IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    indexConfig->Deserialize(any, 0, resource);
    indexConfig->SetTTL(ttl);
    indexConfig->GetValueConfig()->EnableCompactFormat(true);

    _indexConfig = std::move(indexConfig);
}

void BuildingKKVSegmentReaderTest::CheckResultBuffer(const std::vector<uint64_t>& expectedSKeys,
                                                     const std::vector<uint32_t>& expectedValues,
                                                     const std::vector<uint32_t>& expectedTs,
                                                     KKVResultBuffer& resultBuffer)
{
    int cursor = 0;
    while (resultBuffer.IsValid()) {
        ASSERT_EQ(resultBuffer.GetCurrentSkey(), expectedSKeys[cursor]);
        ASSERT_EQ(resultBuffer.GetCurrentTimestamp(), expectedTs[cursor]);
        autil::StringView value;
        resultBuffer.GetCurrentValue(value);
        ASSERT_EQ(*((uint32_t*)value.data()), expectedValues[cursor]);
        resultBuffer.MoveToNext();
        ++cursor;
    }
}

template <typename SKeyType>
void BuildingKKVSegmentReaderTest::DoTestProcess(FieldType skeyType, SKeyType __for_compile)
{
    (void)__for_compile;

    MakeSchema(skeyType, 2);
    _indexConfig->EnableStoreExpireTime();

    ASSERT_TRUE(_indexConfig->StoreExpireTime());
    auto pool = std::make_shared<autil::mem_pool::Pool>();

    using TableType = ClosedHashPrefixKeyTable<SKeyListInfo, PKeyTableType::DENSE>;
    auto fakeTable = std::make_shared<TableType>(1024 * 32, 50);
    fakeTable->Open(nullptr, PKeyTableOpenType::RW);
    using SKeyWriterType = SKeyWriter<SKeyType>;
    using SKeyNodeType = typename SKeyWriterType::SKeyNode;
    auto fakeSKeyWriter = std::make_shared<SKeyWriterType>();
    fakeSKeyWriter->Init(1024 * 1024, 100);
    auto fakeValueWriter = std::make_shared<KKVValueWriter>(pool.get(), false);
    ASSERT_TRUE(fakeValueWriter->Init(1024 * 1024, 1).IsOK());
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;

    static const size_t SKEY_NODE_COUNT = 5;
    std::vector<SKeyNodeType> skeyNodes;
    std::vector<uint32_t> skeyOffsets;
    std::vector<SKeyType> skeys;
    std::vector<StringView> values;
    skeyNodes.reserve(SKEY_NODE_COUNT);
    skeyOffsets.reserve(SKEY_NODE_COUNT);
    skeys.reserve(SKEY_NODE_COUNT);
    values.reserve(SKEY_NODE_COUNT);
    for (size_t i = 0; i < SKEY_NODE_COUNT; ++i) {
        auto value = std::to_string(i);
        values.push_back(value);
        size_t valueOffset = 0;
        ASSERT_TRUE(fakeValueWriter->Append(value, valueOffset).IsOK());
        SKeyType skey = static_cast<SKeyType>(i);
        skeys.push_back(skey);
        uint32_t timestamp = static_cast<uint32_t>(i + 1);
        uint32_t expireTime = static_cast<uint32_t>(i + 100);
        auto skeyNode = SKeyNodeType(skey, valueOffset, timestamp, expireTime, index::INVALID_SKEY_OFFSET);
        skeyNodes.push_back(skeyNode);
        uint32_t skeyOffset = fakeSKeyWriter->Append(skeyNode);
        skeyOffsets.push_back(skeyOffset);
    }

    SKeyListInfo listInfo(skeyOffsets[0], index::INVALID_SKEY_OFFSET, 1);
    for (size_t i = 1; i < SKEY_NODE_COUNT; ++i) {
        fakeSKeyWriter->LinkSkeyNode(listInfo, skeyOffsets[i]);
    }

    // test Normal get result
    {
        KKVBuildingSegmentIterator<SKeyType> iter(fakeSKeyWriter, listInfo, fakeValueWriter, pool.get(),
                                                  _indexConfig.get());
        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys(pool.get());

        ASSERT_FALSE(iter.HasPKeyDeleted());
        ASSERT_TRUE(iter.IsValid());

        KKVResultBuffer resultBuffer(1024 * 1024, 10, pool.get());
        iter.BatchGet(nullptr, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        ASSERT_TRUE(resultBuffer.IsValid());
        ASSERT_EQ(resultBuffer.Size(), SKEY_NODE_COUNT);
        ASSERT_FALSE(resultBuffer.IsFull());

        // check result value
        for (size_t i = 0; i < SKEY_NODE_COUNT; ++i) {
            ASSERT_TRUE(resultBuffer.IsValid());
            ASSERT_EQ(resultBuffer.GetCurrentSkey(), skeys[i]) << "skey=" << skeys[i];
            resultBuffer.MoveToNext();
        }
    }

    // test Get with skey filter
    {
        KKVBuildingSegmentIterator<SKeyType> iter(fakeSKeyWriter, listInfo, fakeValueWriter, pool.get(),
                                                  _indexConfig.get());
        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys(pool.get());

        ASSERT_FALSE(iter.HasPKeyDeleted());
        ASSERT_TRUE(iter.IsValid());

        SKeyContext skeyContext(pool.get());
        std::vector<uint64_t> requireKeys;
        requireKeys.push_back(static_cast<uint64_t>(skeys[0]));
        skeyContext.Init(requireKeys);
        KKVResultBuffer resultBuffer(1024 * 1024, 10, pool.get());
        iter.BatchGet(&skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        ASSERT_TRUE(resultBuffer.IsValid());
        ASSERT_EQ(resultBuffer.Size(), 1);
        ASSERT_EQ(resultBuffer.GetCurrentSkey(), skeys[0]) << "skey=" << resultBuffer.GetCurrentSkey();
    }
}

TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeInt8) { DoTestProcess<int8_t>(ft_int8, 0); }
TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeInt16) { DoTestProcess<int16_t>(ft_int16, 0); }
TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeInt32) { DoTestProcess<int32_t>(ft_int32, 0); }
TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeInt64) { DoTestProcess<int64_t>(ft_int64, 0); }
TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeUInt8) { DoTestProcess<uint8_t>(ft_uint8, 0); }
TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeUInt16) { DoTestProcess<uint16_t>(ft_uint16, 0); }
TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeUInt32) { DoTestProcess<uint32_t>(ft_uint32, 0); }
TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeUInt64) { DoTestProcess<uint64_t>(ft_uint64, 0); }
TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeFloat) { DoTestProcess<float>(ft_float, 0); }
TEST_F(BuildingKKVSegmentReaderTest, TestProcessWithSKeyTypeDouble) { DoTestProcess<double>(ft_double, 0); }

TEST_F(BuildingKKVSegmentReaderTest, TestDeletedPK)
{
    MakeSchema(ft_int32, 2);
    _indexConfig->EnableStoreExpireTime();
    ASSERT_TRUE(_indexConfig->StoreExpireTime());
    auto pool = std::make_shared<autil::mem_pool::Pool>();

    using SKeyType = int32_t;
    using TableType = ClosedHashPrefixKeyTable<SKeyListInfo, PKeyTableType::DENSE>;
    auto fakeTable = std::make_shared<TableType>(1024 * 32, 50);
    fakeTable->Open(nullptr, PKeyTableOpenType::RW);
    using SKeyWriterType = SKeyWriter<SKeyType>;
    using SKeyNodeType = typename SKeyWriterType::SKeyNode;
    auto fakeSKeyWriter = std::make_shared<SKeyWriterType>();
    fakeSKeyWriter->Init(1024 * 1024, 100);
    auto fakeValueWriter = std::make_shared<KKVValueWriter>(pool.get(), false);
    ASSERT_TRUE(fakeValueWriter->Init(1024 * 1024, 1).IsOK());
    uint64_t timestamp = 111;
    auto skeyNode = SKeyNodeType(std::numeric_limits<SKeyType>::min(), SKEY_ALL_DELETED_OFFSET, timestamp, 0,
                                 index::INVALID_SKEY_OFFSET);
    uint32_t skeyOffset = fakeSKeyWriter->Append(skeyNode);

    SKeyListInfo listInfo(skeyOffset, index::INVALID_SKEY_OFFSET, 1);

    KKVBuildingSegmentIterator<SKeyType> iter(fakeSKeyWriter, listInfo, fakeValueWriter, pool.get(),
                                              _indexConfig.get());

    ASSERT_EQ(iter.GetPKeyDeletedTs(), timestamp);
    ASSERT_TRUE(iter.HasPKeyDeleted());
    ASSERT_FALSE(iter.IsValid());
}

TEST_F(BuildingKKVSegmentReaderTest, TestBatchGet)
{
    using SKeyType = uint64_t;
    using SKeyWriterTyped = SKeyWriter<SKeyType>;
    using SKeyNode = SKeyWriterTyped::SKeyNode;
    using SKeyContext = SKeySearchContext<SKeyType>;
    using PooledSKeySet = typename SKeyContext::PooledSKeySet;

    int64_t reserveSize = 1024 * sizeof(SKeyNode);
    size_t longTailThreshold = 100;
    auto skeyWriter = std::make_shared<SKeyWriterTyped>();
    skeyWriter->Init(reserveSize, longTailThreshold);

    vector<SKeyNode> skeyNodes;
    SKeyListInfo listInfo = SKeyListInfo(0, index::INVALID_SKEY_OFFSET, 1);

    auto valueWriter = std::make_shared<KKVValueWriter>(false);
    ASSERT_TRUE(valueWriter->Init(32 * 1024 * 1024, 1).IsOK());

    MakeSchema(ft_uint64, 1024);
    ASSERT_EQ(4, _indexConfig->GetValueConfig()->GetFixedLength());

    for (uint32_t i = 0; i < 4; ++i) {
        uint64_t valueOffset = 0;
        autil::StringView value((const char*)&i, sizeof(i));
        ASSERT_TRUE(valueWriter->Append(value, valueOffset).IsOK());
        auto skeyNode = SKeyNode((SKeyType)i, valueOffset, i, i, index::INVALID_SKEY_OFFSET);
        uint32_t skeyOffset = skeyWriter->Append(skeyNode);
        skeyNodes.push_back(skeyNode);
        if (i > 0) {
            skeyWriter->LinkSkeyNode(listInfo, skeyOffset);
        }
    }
    autil::mem_pool::Pool pool;
    // Test reach limit
    {
        KKVBuildingSegmentIterator<SKeyType> iter(skeyWriter, listInfo, valueWriter, &pool, _indexConfig.get());

        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(&pool)};
        KKVResultBuffer resultBuffer {64, 2, &pool};
        iter.BatchGet(nullptr, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);

        ASSERT_EQ(resultBuffer.Size(), 2);
        std::vector<uint64_t> expectedSKeys = {0, 1};
        std::vector<uint32_t> expectedValues = {0, 1};
        std::vector<uint32_t> expectedTs = {0, 1};
        CheckResultBuffer(expectedSKeys, expectedValues, expectedTs, resultBuffer);
        ASSERT_EQ(foundSkeys.size(), 2);
        ASSERT_NE(foundSkeys.find(0), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(1), foundSkeys.end());
        ASSERT_FALSE(iter.IsValid());
    }

    // Test buffer full
    {
        KKVBuildingSegmentIterator<uint64_t> iter(skeyWriter, listInfo, valueWriter, &pool, _indexConfig.get());

        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(&pool)};
        KKVResultBuffer resultBuffer {2, 1024, &pool};
        iter.BatchGet(nullptr, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        ASSERT_EQ(resultBuffer.Size(), 2);
        std::vector<uint64_t> expectedSKeys = {0, 1};
        std::vector<uint32_t> expectedValues = {0, 1};
        std::vector<uint32_t> expectedTs = {0, 1};
        CheckResultBuffer(expectedSKeys, expectedValues, expectedTs, resultBuffer);
        ASSERT_EQ(foundSkeys.size(), 2);
        ASSERT_NE(foundSkeys.find(0), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(1), foundSkeys.end());
        ASSERT_TRUE(iter.IsValid());
        ASSERT_EQ(2, iter.GetCurrentSkey());
        ASSERT_EQ(2, iter.GetCurrentTs());
        ASSERT_EQ(2, iter.GetCurrentExpireTime());
    }

    // Test normal finish
    {
        KKVBuildingSegmentIterator<uint64_t> iter(skeyWriter, listInfo, valueWriter, &pool, _indexConfig.get());

        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(&pool)};
        KKVResultBuffer resultBuffer {1024, 1024, &pool};
        iter.BatchGet(nullptr, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        std::vector<uint64_t> expectedSKeys = {0, 1, 2, 3};
        std::vector<uint32_t> expectedValues = {0, 1, 2, 3};
        std::vector<uint32_t> expectedTs = {0, 1, 2, 3};
        CheckResultBuffer(expectedSKeys, expectedValues, expectedTs, resultBuffer);
        ASSERT_EQ(foundSkeys.size(), 4);
        ASSERT_NE(foundSkeys.find(0), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(1), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(2), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(3), foundSkeys.end());
        ASSERT_FALSE(iter.IsValid());
    }

    // Test not existed required skey
    {
        KKVBuildingSegmentIterator<uint64_t> iter(skeyWriter, listInfo, valueWriter, &pool, _indexConfig.get());

        SKeySearchContext<SKeyType> skeyContext(&pool);
        skeyContext.Init(vector<uint64_t> {(uint64_t)4});
        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(&pool)};
        KKVResultBuffer resultBuffer {1024, 1024, &pool};
        iter.BatchGet(&skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        ASSERT_EQ(resultBuffer.Size(), 0);
        ASSERT_EQ(foundSkeys.size(), 0);
        ASSERT_FALSE(iter.IsValid());
    }

    // Test required skeys
    {
        KKVBuildingSegmentIterator<uint64_t> iter(skeyWriter, listInfo, valueWriter, &pool, _indexConfig.get());

        SKeySearchContext<SKeyType> skeyContext(&pool);
        skeyContext.Init(vector<uint64_t> {(uint64_t)0, (uint64_t)3});
        uint64_t minimumTsInSecond = 0;
        uint64_t currentTsInSecond = 0;
        PooledSKeySet foundSkeys {autil::mem_pool::pool_allocator<SKeyType>(&pool)};

        KKVResultBuffer resultBuffer {1024, 1024, &pool};
        iter.BatchGet(&skeyContext, minimumTsInSecond, currentTsInSecond, foundSkeys, resultBuffer);
        ASSERT_EQ(resultBuffer.Size(), 2);
        ASSERT_EQ(foundSkeys.size(), 2);
        ASSERT_NE(foundSkeys.find(0), foundSkeys.end());
        ASSERT_NE(foundSkeys.find(3), foundSkeys.end());
        ASSERT_FALSE(iter.IsValid());
    }
}

} // namespace indexlibv2::index
