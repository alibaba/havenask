#include "indexlib/index/kkv/built/KKVDiskIndexer.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
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
#include "indexlib/table/kkv_table/KKVSchemaResolver.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlibv2::index {

class KKVDiskIndexerTest : public TESTBASE
{
public:
    void setUp() override;
    void PrepareSchema();

private:
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KKVDiskIndexerTest);

void KKVDiskIndexerTest::setUp()
{
    _pool.reset(new Pool());

    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", rootPath, fsOptions).GetOrThrow();
    _rootDir = indexlib::file_system::IDirectory::Get(fs);
}

void KKVDiskIndexerTest::PrepareSchema()
{
    string schemaJsonStr = R"({
        "attributes" : [ "single_int16", "single_int32", "single_int64" ],
        "fields" : [
            {"field_type" : "UINT64", "field_name" : "pkey"},
            {"field_type" : "UINT64", "field_name" : "skey"},
            {"field_type" : "INT16", "field_name" : "single_int16"},
            {"field_type" : "INT32", "field_name" : "single_int32"},
            {"field_type" : "INT64", "field_name" : "single_int64"}
        ],
        "indexs" : [ {
            "index_name" : "pkey_skey",
            "index_type" : "PRIMARY_KEY",
            "index_fields" :
                [ {"field_name" : "pkey", "key_type" : "prefix"}, {"field_name" : "skey", "key_type" : "suffix"} ],
            "value_fields" : [
                {"field_type" : "INT16", "field_name" : "single_int16"},
                {"field_type" : "INT32", "field_name" : "single_int32"},
                {"field_type" : "INT64", "field_name" : "single_int64"}
            ]
        } ],
        "table_name" : "kkv_table",
        "table_type" : "kkv"
    })";

    _schema.reset(framework::TabletSchemaLoader::LoadSchema(schemaJsonStr).release());
    ASSERT_TRUE(_schema);
    auto kkvIndexConfigs = _schema->GetIndexConfigs();
    ASSERT_EQ(1, kkvIndexConfigs.size());

    _indexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(kkvIndexConfigs[0]);
    ASSERT_NE(nullptr, _indexConfig);

    _indexConfig->GetIndexPreference().GetHashDictParam().SetHashType(KKV_PKEY_DENSE_TABLE_NAME);
}

TEST_F(KKVDiskIndexerTest, TestSimpleProcess)
{
    PrepareSchema();
    _indexConfig->SetUseNumberHash(true);

    KKVMemIndexer<int32_t> memIndexer("test", 1024, 1024, 1024, 1.0, 1.0, true, true);
    ASSERT_TRUE(memIndexer.Init(_indexConfig, nullptr).IsOK());
    string docStr = "cmd=add,pkey=1,skey=1,value=1,ts=100000000;"
                    "cmd=add,pkey=1,skey=2,value=2,ts=200000000;"
                    "cmd=add,pkey=2,skey=21,value=1,ts=300000000;";

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
    auto indexDirectory = _rootDir->MakeDirectory("dump", indexlib::file_system::DirectoryOption::Mem()).GetOrThrow();
    auto legacyIndexDirectory = indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory);
    auto status = memIndexer.Dump(dumpPool.get(), legacyIndexDirectory, dumpParams);
    ASSERT_TRUE(status.IsOK()) << status.ToString();
    ASSERT_TRUE(indexDirectory->IsExist(_indexConfig->GetIndexName()).OK());
    auto result = indexDirectory->GetDirectory(_indexConfig->GetIndexName());
    ASSERT_TRUE(result.OK());
    auto kkvDir = result.Value();
    ASSERT_TRUE(kkvDir);
    ASSERT_TRUE(kkvDir->IsExist("pkey").GetOrThrow());
    ASSERT_TRUE(kkvDir->IsExist("skey").GetOrThrow());
    ASSERT_TRUE(kkvDir->IsExist("value").GetOrThrow());

    // Load by KKVDiskIndexer
    KKVDiskIndexer<int32_t> diskIndexer(100, true);
    {
        auto status = diskIndexer.Open(_indexConfig, indexDirectory);
        ASSERT_TRUE(status.IsOK()) << status.ToString();

        ASSERT_TRUE(diskIndexer.GetReader());
        ASSERT_TRUE(diskIndexer.GetIndexConfig());
        ASSERT_TRUE(diskIndexer.GetIndexDirectory());
    }
    // Get reader
    {
        auto reader = diskIndexer.GetReader();
        ASSERT_TRUE(reader);
        {
            auto [status, iter] = future_lite::interface::syncAwait(reader->Lookup(1, _pool.get()));
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(iter);
        }
        {
            auto [status, iter] = future_lite::interface::syncAwait(reader->Lookup(999, _pool.get()));
            ASSERT_TRUE(status.IsOK());
            ASSERT_FALSE(iter);
        }
    }
}

} // namespace indexlibv2::index
