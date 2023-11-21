#include "indexlib/index/kkv/building/KKVMemIndexer.h"

#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kkv/building/KKVMemIndexer.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/config/test/KKVIndexConfigBuilder.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;

namespace indexlibv2::index {

class KKVMemIndexerTest : public TESTBASE
{
public:
    void setUp() override;

private:
    void DoTestBuildWithPKeyType(FieldType pkeyType);
    void DoTestBuildWithSKeyType(FieldType skeyType);
    void DoTestBuildWithValueType(FieldType valueType);
    void DoTestBuildInternal(FieldType pkeyType, FieldType skeyType, FieldType valueType, int64_t ttl);

private:
    std::string GetCommonDocStrForBuild();
    void MakeSchema(FieldType pkeyType, FieldType skeyType, FieldType valueType, int64_t ttl);
    std::shared_ptr<IMemIndexer> CreateKKVMemIndexer(FieldType skeyType, size_t pkeyMemUse, size_t skeyMemUse,
                                                     size_t valueMemUse, double skeyCompressRatio,
                                                     double valueCompressRatio);

private:
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KKVMemIndexerTest);

void KKVMemIndexerTest::setUp()
{
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::IDirectory::Get(fs);
}

std::string KKVMemIndexerTest::GetCommonDocStrForBuild()
{
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=2,value=2,ts=200000000;"
                    "cmd=add,pkey=1,skey=3,value=3,ts=300000000;"
                    "cmd=delete,pkey=1,skey=3,ts=600000000;" // delete seky
                    "cmd=add,pkey=1,skey=4,value=4,ts=400000000;"
                    "cmd=add,pkey=2,skey=21,value=21,ts=100000000;"
                    "cmd=add,pkey=2,skey=21,value=21,ts=100000000;" // add duplicate skey
                    "cmd=add,pkey=1,skey=5,value=5,ts=500000000;"
                    "cmd=delete,pkey=2,ts=500000000;"
                    "cmd=delete,pkey=2,ts=500000000;"        // delete same pkey twice
                    "cmd=delete,pkey=1,skey=3,ts=600000000;" // delete skey not exist
                    "cmd=delete,pkey=99999,ts=500000000;"    // delete pkey not exist
                    "cmd=add,pkey=2,skey=23,value=23,ts=300000000;"
                    "cmd=add,pkey=2,skey=21,value=21,ts=100000000;"
                    "cmd=add,pkey=3,skey=31,value=31,ts=100000000;"
                    "cmd=add,pkey=3,skey=31,value=31,ts=100000000;"; // add duplicate skey after double delete
    return docStr;
}

void KKVMemIndexerTest::MakeSchema(FieldType pkeyType, FieldType skeyType, FieldType valueType, int64_t ttl)
{
    std::string pkeyTypeStr(indexlibv2::config::FieldConfig::FieldTypeToStr(pkeyType));
    std::string skeyTypeStr(indexlibv2::config::FieldConfig::FieldTypeToStr(skeyType));
    std::string valueTypeStr(indexlibv2::config::FieldConfig::FieldTypeToStr(valueType));

    std::string fields = "pkey:" + pkeyTypeStr + ";skey:" + skeyTypeStr + ";value:" + valueTypeStr;
    auto [schema, indexConfig] = KKVIndexConfigBuilder::MakeIndexConfig(fields, "pkey", "skey", "value", ttl);
    ASSERT_TRUE(schema);
    ASSERT_TRUE(indexConfig);
    _schema = std::move(schema);
    _indexConfig = std::move(indexConfig);
}

void KKVMemIndexerTest::DoTestBuildWithPKeyType(FieldType pkeyType)
{
    // test table type PKeyTableType::DENSE
    {
        MakeSchema(pkeyType, ft_int8, ft_int8, -1);
        _indexConfig->GetIndexPreference().GetHashDictParam().SetHashType(KKV_PKEY_DENSE_TABLE_NAME);
        this->DoTestBuildInternal(pkeyType, ft_int8, ft_int8, -1);
    }

    // test table type PKeyTableType::SEPARATE_CHAIN
    {
        MakeSchema(pkeyType, ft_int8, ft_int8, -1);
        _indexConfig->GetIndexPreference().GetHashDictParam().SetHashType(KKV_PKEY_SEPARATE_CHAIN_TABLE_NAME);
        this->DoTestBuildInternal(pkeyType, ft_int8, ft_int8, -1);
    }
}

void KKVMemIndexerTest::DoTestBuildWithSKeyType(FieldType skeyType)
{
    this->DoTestBuildInternal(ft_int8, skeyType, ft_int8, -1);
    this->DoTestBuildInternal(ft_int8, skeyType, ft_int8, 1024);
}

void KKVMemIndexerTest::DoTestBuildWithValueType(FieldType valueType)
{
    {
        this->DoTestBuildInternal(ft_int8, ft_int8, valueType, -1);
        this->DoTestBuildInternal(ft_int8, ft_int8, valueType, 1024);
    }

    // test value impact
    {
        MakeSchema(ft_int8, ft_int8, valueType, -1);
        _indexConfig->GetIndexPreference().GetValueParam().EnableValueImpact(true);
        this->DoTestBuildInternal(ft_int8, ft_int8, valueType, 1024);
    }
}

std::shared_ptr<IMemIndexer> KKVMemIndexerTest::CreateKKVMemIndexer(FieldType skeyType, size_t pkeyMemUse,
                                                                    size_t skeyMemUse, size_t valueMemUse,
                                                                    double skeyCompressRatio, double valueCompressRatio)
{
    const std::string& tabletName = "test";
    FieldType skeyDictType = skeyType == ft_string ? ft_uint64 : skeyType;
    switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        using SKeyType = indexlib::index::FieldTypeTraits<type>::AttrItemType;                                         \
        return std::make_shared<KKVMemIndexer<SKeyType>>(tabletName, pkeyMemUse, skeyMemUse, valueMemUse,              \
                                                         skeyCompressRatio, valueCompressRatio, true, true);           \
    }
        KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(skeyDictType));
        return nullptr;
    }
    }
}

void KKVMemIndexerTest::DoTestBuildInternal(FieldType pkeyType, FieldType skeyType, FieldType valueType, int64_t ttl)
{
    if (!_indexConfig) {
        MakeSchema(pkeyType, skeyType, valueType, ttl);
    }
    // normal build
    {
        auto indexer = this->CreateKKVMemIndexer(skeyType, 1024, 1024, 1024, 1.0, 1.0);
        ASSERT_TRUE(indexer->Init(_indexConfig, nullptr).IsOK());
        string docStr = GetCommonDocStrForBuild();
        auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
        // Build
        for (const auto& rawDoc : rawDocs) {
            auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
            ASSERT_TRUE(docBatch);
            auto s = indexer->Build(docBatch.get());
            ASSERT_TRUE(s.IsOK()) << s.ToString();
        }
    }
}

// test different pkey type
TEST_F(KKVMemIndexerTest, TestBuildWithPKeyTypeInt8) { DoTestBuildWithPKeyType(ft_int8); }
TEST_F(KKVMemIndexerTest, TestBuildWithPKeyTypeInt16) { DoTestBuildWithPKeyType(ft_int16); }
TEST_F(KKVMemIndexerTest, TestBuildWithPKeyTypeInt32) { DoTestBuildWithPKeyType(ft_int32); }
TEST_F(KKVMemIndexerTest, TestBuildWithPKeyTypeInt64) { DoTestBuildWithPKeyType(ft_int64); }
TEST_F(KKVMemIndexerTest, TestBuildWithPKeyTypeUInt8) { DoTestBuildWithPKeyType(ft_uint8); }
TEST_F(KKVMemIndexerTest, TestBuildWithPKeyTypeUInt16) { DoTestBuildWithPKeyType(ft_uint16); }
TEST_F(KKVMemIndexerTest, TestBuildWithPKeyTypeUInt32) { DoTestBuildWithPKeyType(ft_uint32); }
TEST_F(KKVMemIndexerTest, TestBuildWithPKeyTypeUInt64) { DoTestBuildWithPKeyType(ft_uint64); }
TEST_F(KKVMemIndexerTest, TestBuildWithPKeyTypeString) { DoTestBuildWithPKeyType(ft_string); }

// test different skey type
TEST_F(KKVMemIndexerTest, TestBuildWithSKeyTypeInt8) { DoTestBuildWithSKeyType(ft_int8); }
TEST_F(KKVMemIndexerTest, TestBuildWithSKeyTypeInt16) { DoTestBuildWithSKeyType(ft_int16); }
TEST_F(KKVMemIndexerTest, TestBuildWithSKeyTypeInt32) { DoTestBuildWithSKeyType(ft_int32); }
TEST_F(KKVMemIndexerTest, TestBuildWithSKeyTypeInt64) { DoTestBuildWithSKeyType(ft_int64); }
TEST_F(KKVMemIndexerTest, TestBuildWithSKeyTypeUInt8) { DoTestBuildWithSKeyType(ft_uint8); }
TEST_F(KKVMemIndexerTest, TestBuildWithSKeyTypeUInt16) { DoTestBuildWithSKeyType(ft_uint16); }
TEST_F(KKVMemIndexerTest, TestBuildWithSKeyTypeUInt32) { DoTestBuildWithSKeyType(ft_uint32); }
TEST_F(KKVMemIndexerTest, TestBuildWithSKeyTypeUInt64) { DoTestBuildWithSKeyType(ft_uint64); }
TEST_F(KKVMemIndexerTest, TestBuildWithSKeyTypeString) { DoTestBuildWithSKeyType(ft_string); }

// test different value type
TEST_F(KKVMemIndexerTest, TestBuildWithValueTypeInt8) { DoTestBuildWithValueType(ft_int8); }
TEST_F(KKVMemIndexerTest, TestBuildWithValueTypeInt16) { DoTestBuildWithValueType(ft_int16); }
TEST_F(KKVMemIndexerTest, TestBuildWithValueTypeInt32) { DoTestBuildWithValueType(ft_int32); }
TEST_F(KKVMemIndexerTest, TestBuildWithValueTypeInt64) { DoTestBuildWithValueType(ft_int64); }
TEST_F(KKVMemIndexerTest, TestBuildWithValueTypeUInt8) { DoTestBuildWithValueType(ft_uint8); }
TEST_F(KKVMemIndexerTest, TestBuildWithValueTypeUInt16) { DoTestBuildWithValueType(ft_uint16); }
TEST_F(KKVMemIndexerTest, TestBuildWithValueTypeUInt32) { DoTestBuildWithValueType(ft_uint32); }
TEST_F(KKVMemIndexerTest, TestBuildWithValueTypeUInt64) { DoTestBuildWithValueType(ft_uint64); }
TEST_F(KKVMemIndexerTest, TestBuildWithValueTypeString) { DoTestBuildWithValueType(ft_string); }

TEST_F(KKVMemIndexerTest, TestBuildWithoutEnoughMemory)
{
    MakeSchema(ft_int32, ft_int32, ft_int32, -1);
    auto indexer = this->CreateKKVMemIndexer(ft_int32, 1, 1, 1, 1.0, 1.0);
    ASSERT_TRUE(indexer->Init(_indexConfig, nullptr).IsOK());
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=2,value=2,ts=200000000;"
                    "cmd=add,pkey=1,skey=3,value=3,ts=300000000;";

    auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
    // Build
    for (int i = 0, size = rawDocs.size(); i < size; ++i) {
        const auto& rawDoc = rawDocs[i];
        auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
        ASSERT_TRUE(docBatch);
        auto s = indexer->Build(docBatch.get());
        // TODO(qisa.cb) should fix pkey table and suffix writer action
        // 1、pkey table always set pkey_count = 256 * 1024 when pass pkey_count = 0
        // 2、skey writer capacity always >=1 when pass memory size = 0
        if (i > 0) {
            ASSERT_TRUE(s.IsNeedDump()) << s.ToString();
        }
    }
}

TEST_F(KKVMemIndexerTest, TestCreateInMemoryReader)
{
    MakeSchema(ft_int32, ft_int32, ft_int32, -1);
    auto indexer = this->CreateKKVMemIndexer(ft_int32, 1024, 1024, 1024, 1.0, 1.0);
    ASSERT_TRUE(indexer->Init(_indexConfig, nullptr).IsOK());
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=2,value=2,ts=200000000;"
                    "cmd=add,pkey=2,skey=21,value=1,ts=300000000;";

    auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
    // Build
    for (const auto& rawDoc : rawDocs) {
        auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
        ASSERT_TRUE(docBatch);
        auto s = indexer->Build(docBatch.get());
        ASSERT_TRUE(s.IsOK()) << s.ToString();
    }
    auto indexerTyped = dynamic_pointer_cast<KKVMemIndexer<int32_t>>(indexer);
    ASSERT_TRUE(indexerTyped);
    auto reader = indexerTyped->CreateInMemoryReader();
    ASSERT_TRUE(reader);

    auto pool = std::make_shared<autil::mem_pool::UnsafePool>(1024 * 1024);
    const auto& rawDoc = rawDocs[0];
    auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
    ASSERT_TRUE(docBatch);
    document::KKVDocumentBatch* kvDocBatch = dynamic_cast<document::KKVDocumentBatch*>(docBatch.get());
    ASSERT_TRUE(kvDocBatch);
    auto kvDocTyped = std::dynamic_pointer_cast<document::KKVDocument>(kvDocBatch->TEST_GetDocument(0));
    ASSERT_TRUE(kvDocTyped);
    {
        auto iter = reader->Lookup(kvDocTyped->GetPKeyHash(), pool.get());
        ASSERT_TRUE(iter);
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(pool.get(), iter);
    }
    {
        auto iter = reader->Lookup(999, pool.get());
        ASSERT_FALSE(iter);
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(pool.get(), iter);
    }
}

TEST_F(KKVMemIndexerTest, TestDump)
{
    MakeSchema(ft_int32, ft_int32, ft_int32, -1);
    auto indexer = this->CreateKKVMemIndexer(ft_int32, 1024, 1024, 1024, 1.0, 1.0);
    ASSERT_TRUE(indexer->Init(_indexConfig, nullptr).IsOK());
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=2,value=2,ts=200000000;"
                    "cmd=add,pkey=2,skey=21,value=1,ts=300000000;";

    auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
    // Build
    for (const auto& rawDoc : rawDocs) {
        auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
        ASSERT_TRUE(docBatch);
        auto s = indexer->Build(docBatch.get());
        ASSERT_TRUE(s.IsOK()) << s.ToString();
    }

    auto dumpPool = std::make_shared<autil::mem_pool::UnsafePool>(1024 * 1024);
    std::shared_ptr<framework::DumpParams> dumpParams;
    auto indexDirectory = _rootDir->MakeDirectory("dump", indexlib::file_system::DirectoryOption::Mem()).GetOrThrow();
    auto status =
        indexer->Dump(dumpPool.get(), indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory), dumpParams);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_TRUE(indexDirectory->IsExist(_indexConfig->GetIndexName()).OK());
    auto result = indexDirectory->GetDirectory(_indexConfig->GetIndexName());
    auto kkvDir = result.Value();
    ASSERT_TRUE(kkvDir);
    ASSERT_TRUE(kkvDir->IsExist("pkey").GetOrThrow());
    ASSERT_TRUE(kkvDir->IsExist("skey").GetOrThrow());
    ASSERT_TRUE(kkvDir->IsExist("value").GetOrThrow());
}

} // namespace indexlibv2::index
