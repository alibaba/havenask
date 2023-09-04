#include "indexlib/index/kkv/dump/KKVIndexDumper.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/document/kkv/KKVDocumentBatch.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/kkv/building/KKVMemIndexer.h"
#include "indexlib/index/kkv/built/KKVDiskIndexer.h"
#include "indexlib/index/kkv/common/NormalOnDiskSKeyNode.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableCreator.h"
#include "indexlib/table/kkv_table/KKVSchemaResolver.h"
#include "unittest/unittest.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::util;
using namespace indexlib::file_system;
namespace indexlibv2::index {

class KKVIndexDumperTest : public virtual TESTBASE_BASE, public virtual testing::TestWithParam<PKeyTableType>
{
public:
    void SetUp() override;
    void TearDown() override { TESTBASE_BASE::TearDown(); }

private:
    void DoTest(bool isOnline, bool compressSKey, bool compressValue);
    std::string MakeDocs(const std::map<uint64_t, std::vector<std::pair<uint64_t, int32_t>>>& pk2skval);
    void BuildIndex(KKVMemIndexer<uint64_t>& indexer, const std::string& docs);
    void DumpIndex(KKVMemIndexer<uint64_t>& indexer, const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                   bool isOnline);
    void CheckIndex(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                    const std::map<uint64_t, std::vector<std::pair<uint64_t, int32_t>>>& pk2skval, bool isOnline);

private:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<config::ITabletSchema> _tableSchema;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KKVIndexDumperTest);

INSTANTIATE_TEST_CASE_P(p1, KKVIndexDumperTest,
                        testing::Values(index::PKeyTableType::DENSE, index::PKeyTableType::CUCKOO,
                                        index::PKeyTableType::SEPARATE_CHAIN));

void KKVIndexDumperTest::SetUp()
{
    TESTBASE_BASE::SetUp();

    string schemaJsonStr = R"({
        "attributes" : ["single_int32"],
        "fields" : [
            {"field_type" : "UINT64", "field_name" : "pkey"},
            {"field_type" : "UINT64", "field_name" : "skey"},
            {"field_type" : "INT32", "field_name" : "single_int32"}
        ],
        "indexs" : [ {
            "index_name" : "pkey_skey",
            "index_type" : "PRIMARY_KEY",
            "index_fields" :
                [ {"field_name" : "pkey", "key_type" : "prefix"}, {"field_name" : "skey", "key_type" : "suffix"} ],
            "value_fields" : [
                {"field_type" : "INT32", "field_name" : "single_int32"}
            ]
        } ],
        "table_name" : "kkv_table",
        "table_type" : "kkv"
    })";

    _tableSchema.reset(framework::TabletSchemaLoader::LoadSchema(schemaJsonStr).release());
    ASSERT_TRUE(_tableSchema);
    auto kkvIndexConfigs = _tableSchema->GetIndexConfigs();
    ASSERT_EQ(1, kkvIndexConfigs.size());

    _indexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(kkvIndexConfigs[0]);
    ASSERT_NE(nullptr, _indexConfig);

    PKeyTableType tableType = GetParam();
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(tableType));
    _indexConfig->GetIndexPreference().SetHashDictParam(param);
}

void KKVIndexDumperTest::DoTest(bool isOnline, bool compressSKey, bool compressValue)
{
    if (compressSKey) {
        KKVIndexPreference::SuffixKeyParam param;
        param.SetFileCompressType("snappy");
        _indexConfig->GetIndexPreference().SetSkeyParam(param);
    }
    if (compressValue) {
        KKVIndexPreference::ValueParam param;
        param.SetFileCompressType("snappy");
        _indexConfig->GetIndexPreference().SetValueParam(param);
    }

    std::map<uint64_t, std::vector<std::pair<uint64_t, int32_t>>> pk2skval;
    pk2skval[0l].emplace_back(0l, 0l);
    pk2skval[0l].emplace_back(1l, 10l);
    pk2skval[0l].emplace_back(2l, 20l);
    pk2skval[99].emplace_back(3l, 30l);
    pk2skval[99].emplace_back(4l, 40l);
    pk2skval[99].emplace_back(5l, 50l);
    pk2skval[10000].emplace_back(6l, 60l);
    pk2skval[20000].emplace_back(7l, 70l);
    pk2skval[300000l].emplace_back(8l, 80l);
    pk2skval[400000l].emplace_back(9l, 90l);
    pk2skval[500000l].emplace_back(9l, 90l);
    pk2skval[600000l].emplace_back(9l, 90l);
    string docs = MakeDocs(pk2skval);

    size_t maxPKeyMemUse = 4 * 1024 * 1024;
    size_t maxSKeyMemUse = 1 * 1024 * 1024;
    size_t maxValueMemUse = 1 * 1024 * 1024;
    KKVMemIndexer<uint64_t> indexer("test", maxPKeyMemUse, maxSKeyMemUse, maxValueMemUse, 1.0f, 1.0f, true);
    ASSERT_TRUE(indexer.Init(_indexConfig, nullptr).IsOK());
    BuildIndex(indexer, docs);

    auto fs = indexlib::file_system::FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(),
                                                               indexlib::file_system::FileSystemOptions::Offline())
                  .GetOrThrow();
    auto segmentDirectory = indexlib::file_system::IDirectory::Get(fs);
    DumpIndex(indexer, segmentDirectory, isOnline);
    CheckIndex(segmentDirectory, pk2skval, isOnline);
}

std::string KKVIndexDumperTest::MakeDocs(const std::map<uint64_t, std::vector<std::pair<uint64_t, int32_t>>>& pk2skval)
{
    stringstream ss;
    for (auto iterator = pk2skval.rbegin(); iterator != pk2skval.rend(); ++iterator) {
        const auto& [pk, skval] = *iterator;
        for (const auto& [skey, val] : skval) {
            ss << "cmd=add,single_int32=" << val << ","
               << "pkey=" << pk << ","
               << "skey=" << skey << ","
               << "ts=" << 0 << ";";
        }
    }
    return ss.str();
}

void KKVIndexDumperTest::BuildIndex(KKVMemIndexer<uint64_t>& indexer, const std::string& docStr)
{
    auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
    for (const auto& rawDoc : rawDocs) {
        auto batch = document::KVDocumentBatchMaker::Make(_tableSchema, {rawDoc});
        ASSERT_TRUE(indexer.Build(batch.get()).IsOK());
    }
}

void KKVIndexDumperTest::DumpIndex(KKVMemIndexer<uint64_t>& indexer,
                                   const std::shared_ptr<indexlib::file_system::IDirectory>& segmentDirectory,
                                   bool isOnline)
{
    auto dumpDirectory =
        segmentDirectory->MakeDirectory(_indexConfig->GetIndexName(), indexlib::file_system::DirectoryOption())
            .GetOrThrow();
    auto pkeyTable = indexer.TEST_GetPKeyTable();
    auto skeyWriter = indexer.TEST_GetSkeyWriter();
    auto valueWriter = indexer.TEST_GetValueWriter();

    std::vector<std::pair<uint64_t, SKeyListInfo>> nodesBeforeDump;
    {
        auto pkeyIterator = pkeyTable->CreateIterator();
        for (; pkeyIterator->IsValid(); pkeyIterator->MoveToNext()) {
            uint64_t pkey = 0;
            SKeyListInfo listInfo;
            pkeyIterator->Get(pkey, listInfo);
            nodesBeforeDump.emplace_back(pkey, listInfo);
        }
    }

    KKVIndexDumper<uint64_t> dumper(pkeyTable, skeyWriter, valueWriter, _indexConfig, isOnline);
    ASSERT_TRUE(dumper.Dump(dumpDirectory, &_pool).IsOK());
    auto pkeyIterator = pkeyTable->CreateIterator();
    ASSERT_EQ(pkeyIterator->Size(), nodesBeforeDump.size());

    if (!isOnline) {
        return;
    }
    for (size_t i = 0; i < nodesBeforeDump.size(); ++i) {
        uint64_t pkey = 0;
        SKeyListInfo listInfo;
        pkeyIterator->Get(pkey, listInfo);
        ASSERT_EQ(pkey, nodesBeforeDump[i].first);
        ASSERT_EQ(listInfo, nodesBeforeDump[i].second);
        pkeyIterator->MoveToNext();
    }
}

void KKVIndexDumperTest::CheckIndex(const std::shared_ptr<indexlib::file_system::IDirectory>& segmentDirectory,
                                    const std::map<uint64_t, std::vector<std::pair<uint64_t, int32_t>>>& pk2skval,
                                    bool isOnline)
{
    KKVDiskIndexer<uint64_t> indexer(0, false);
    ASSERT_TRUE(indexer.Open(_indexConfig, segmentDirectory).IsOK());
    auto indexReader = indexer.GetReader();

    for (const auto& [pk, skval] : pk2skval) {
        auto [status, iterator] = indexReader->Lookup(pk, &_pool);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(iterator);
        ASSERT_TRUE(iterator->IsValid());
        size_t i = 0;
        for (const auto& [skey, val] : skval) {
            ASSERT_EQ(iterator->GetCurrentSkey(), skey);
            autil::StringView value;
            iterator->GetCurrentValue(value);
            int32_t actualValue = 0;
            memcpy(&actualValue, value.data(), sizeof(actualValue));
            ASSERT_EQ(actualValue, val);

            ++i;
            iterator->MoveToNext();
            ASSERT_TRUE((i != skval.size()) == iterator->IsValid());
        }
        POOL_COMPATIBLE_DELETE_CLASS(&_pool, iterator);
    }

    auto pkTable = indexReader->_pkeyTable;
    uint64_t priorChunkOffset = 0;
    uint64_t priorInChunkOffset = 0;

    bool isSorted = true;
    for (const auto& [pk, _] : pk2skval) {
        auto offset = *pkTable->Find(pk);
        bool offsetInAscOrder =
            (priorChunkOffset < offset.chunkOffset) ||
            ((priorChunkOffset == offset.chunkOffset) && (priorInChunkOffset <= offset.inChunkOffset));
        if (!isOnline) {
            ASSERT_TRUE(offsetInAscOrder);
        } else {
            if (!offsetInAscOrder) {
                isSorted = false;
            }
        }
        priorChunkOffset = offset.chunkOffset;
        priorInChunkOffset = offset.inChunkOffset;
    }
    ASSERT_TRUE(isSorted);
}

TEST_P(KKVIndexDumperTest, TestOnline) { DoTest(true, false, false); }
TEST_P(KKVIndexDumperTest, TestOffline) { DoTest(false, false, false); }
TEST_P(KKVIndexDumperTest, TestCompressSKey) { DoTest(true, true, false); }
TEST_P(KKVIndexDumperTest, TestCompressValue) { DoTest(false, false, true); }
TEST_P(KKVIndexDumperTest, TestCompressSKeyAndValue) { DoTest(false, true, true); }
} // namespace indexlibv2::index
