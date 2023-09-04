#include "indexlib/index/kv/FixedLenKVMemIndexer.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/FixedLenHashTableCreator.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class FixedLenKVMemIndexerTest : public TESTBASE
{
public:
    void setUp() override
    {
        _pool = std::make_unique<autil::mem_pool::Pool>();
        auto testPath = GET_TEMPLATE_DATA_PATH();
        _directory = indexlib::file_system::Directory::GetPhysicalDirectory(testPath);
        ASSERT_TRUE(_directory);
    }

protected:
    void MakeSchema(const std::string& fieldType, int64_t ttl = -1)
    {
        // uses number hash to simplify test
        std::string fields = "key:uint32;value:" + fieldType;
        auto [schema, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig(fields, "key", "value", ttl);
        ASSERT_TRUE(schema);
        ASSERT_TRUE(indexConfig);
        _schema = std::move(schema);
        _indexConfig = std::move(indexConfig);
    }

    template <FieldType ft>
    void doTestBuildAndDump(int64_t ttl = -1)
    {
        std::string fieldTypeStr(indexlibv2::config::FieldConfig::FieldTypeToStr(ft));
        MakeSchema(fieldTypeStr, ttl);
        FixedLenKVMemIndexer indexer(DEFAULT_MEMORY_USE_IN_BYTES);
        ASSERT_TRUE(indexer.Init(_indexConfig, nullptr).IsOK());

        // build
        std::string docStr = "cmd=add,key=1,value=10,ts=101000000;"
                             "cmd=add,key=2,value=20,ts=102000000;"
                             "cmd=add,key=3,value=30,ts=103000000;"
                             "cmd=delete,key=2,ts=104000000;";
        auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
        for (const auto& rawDoc : rawDocs) {
            auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
            ASSERT_TRUE(docBatch);
            auto s = indexer.Build(docBatch.get());
            ASSERT_TRUE(s.IsOK()) << s.ToString();
        }

        // fill and check statistics
        auto metrics = std::make_shared<indexlib::framework::SegmentMetrics>();
        indexer.FillStatistics(metrics);
        auto groupMetrics = metrics->GetSegmentGroupMetrics("key");
        ASSERT_TRUE(groupMetrics);
        ASSERT_EQ(3, groupMetrics->Get<size_t>(SegmentStatistics::KV_KEY_COUNT));
        ASSERT_EQ(1, groupMetrics->Get<size_t>(SegmentStatistics::KV_KEY_DELETE_COUNT));
        ASSERT_EQ(1.0f, groupMetrics->Get<float>(SegmentStatistics::KV_KEY_VALUE_MEM_RATIO));
        ASSERT_LT(0, groupMetrics->Get<size_t>(SegmentStatistics::KV_SEGMENT_MEM_USE));
        ASSERT_LT(0, groupMetrics->Get<size_t>(SegmentStatistics::KV_HASH_MEM_USE));
        ASSERT_EQ(0, groupMetrics->Get<size_t>(SegmentStatistics::KV_VALUE_MEM_USE));
        ASSERT_EQ(groupMetrics->Get<size_t>(SegmentStatistics::KV_SEGMENT_MEM_USE),
                  groupMetrics->Get<size_t>(SegmentStatistics::KV_HASH_MEM_USE) +
                      groupMetrics->Get<size_t>(SegmentStatistics::KV_VALUE_MEM_USE));

        // dump
        auto s = indexer.Dump(_pool.get(), _directory, nullptr);
        ASSERT_TRUE(s.IsOK()) << s.ToString();
        ASSERT_TRUE(_directory->IsExist("key/key"));
        ASSERT_FALSE(_directory->IsExist("key/value"));
        ASSERT_TRUE(_directory->IsExist("key/format_option"));

        // check index
        // TODO: check result using reader
        auto typeId = MakeKVTypeId(*_indexConfig, nullptr);
        ASSERT_FALSE(typeId.isVarLen);
        ASSERT_EQ(typeId.hasTTL, ttl != -1);
        auto hashTableInfo = FixedLenHashTableCreator::CreateHashTableForWriter(typeId);
        ASSERT_TRUE(hashTableInfo);
        ASSERT_TRUE(hashTableInfo->hashTable);
        ASSERT_TRUE(hashTableInfo->valueUnpacker);
        auto fileReader = _directory->CreateFileReader("key/key", indexlib::file_system::FSOT_MEM);
        ASSERT_TRUE(fileReader);
        std::vector<char> data(fileReader->GetLength());
        ASSERT_EQ(fileReader->GetLength(), fileReader->Read(&data[0], data.size(), 0).GetOrThrow());
        HashTableBase* hashTable = dynamic_cast<HashTableBase*>(hashTableInfo->hashTable.get());
        ASSERT_TRUE(hashTable);
        ASSERT_TRUE(hashTable->MountForRead(&data[0], data.size()));
        ASSERT_EQ(3, hashTable->Size());

        autil::StringView str;
        ASSERT_EQ(indexlib::util::OK, hashTable->Find(1, str));
        ASSERT_EQ(indexlib::util::DELETED, hashTable->Find(2, str));
        ASSERT_EQ(indexlib::util::OK, hashTable->Find(3, str));

        // check format
        std::string content;
        _directory->Load("key/format_option", content);
        KVFormatOptions opts;
        opts.FromString(content);
        ASSERT_FALSE(opts.IsShortOffset());
    }

protected:
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _indexConfig;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<indexlib::file_system::Directory> _directory;

protected:
    static constexpr int64_t DEFAULT_MEMORY_USE_IN_BYTES = 1024 * 1024;
};

TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpInt8) { doTestBuildAndDump<ft_int8>(); }
TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpInt16) { doTestBuildAndDump<ft_int16>(); }
TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpInt32) { doTestBuildAndDump<ft_int32>(); }
TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpInt64) { doTestBuildAndDump<ft_int64>(); }
TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpUInt8) { doTestBuildAndDump<ft_uint8>(); }
TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpUInt16) { doTestBuildAndDump<ft_uint16>(); }
TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpUInt32) { doTestBuildAndDump<ft_uint32>(); }
TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpUInt64) { doTestBuildAndDump<ft_uint64>(); }
TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpFloat) { doTestBuildAndDump<ft_float>(); }
TEST_F(FixedLenKVMemIndexerTest, testBuildAndDumpDouble) { doTestBuildAndDump<ft_double>(); }

} // namespace indexlibv2::index
