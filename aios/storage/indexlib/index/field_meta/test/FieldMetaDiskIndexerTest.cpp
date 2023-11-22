#include "indexlib/index/field_meta/FieldMetaDiskIndexer.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/field_meta/FieldMetaMemIndexer.h"
#include "indexlib/index/field_meta/meta/FieldTokenCountMeta.h"
#include "indexlib/index/field_meta/meta/MinMaxFieldMeta.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class FieldMetaDiskIndexerTest : public TESTBASE
{
public:
    FieldMetaDiskIndexerTest() = default;
    ~FieldMetaDiskIndexerTest() = default;

public:
    void setUp() override;
    void tearDown() override;

protected:
    void prepareData(const std::shared_ptr<FieldMetaConfig>& metaConfig);
    IFieldMeta::FieldValueBatch CreateFieldMetaValueBatch(int64_t key, const std::string& value,
                                                          uint32_t fieldTokenCount, bool isNull);
    std::shared_ptr<FieldMetaMemIndexer> CreateMemIndexer(const std::shared_ptr<FieldMetaConfig>& metaConfig);

private:
    indexlibv2::index::DiskIndexerParameter _indexerParam;
    std::shared_ptr<FieldMetaConfig> _metaConfig;
    std::shared_ptr<file_system::Directory> _metaDir;
    std::shared_ptr<indexlibv2::document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
    std::string _rootPath;
};

void FieldMetaDiskIndexerTest::setUp()
{
    std::string jsonStr = R"({
            "field_name" : "price",
            "index_name" : "price",
            "metas" : [ { "metaName" : "MinMax" } ]
        })";

    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("price", ft_int32, /*isMulti*/ false);
    indexlibv2::config::MutableJson runtimeSettings;
    indexlibv2::config::IndexConfigDeserializeResource resource({fieldConfig}, runtimeSettings);
    _metaConfig = std::make_shared<FieldMetaConfig>();
    autil::legacy::Any any = autil::legacy::json::ParseJson(jsonStr);
    _metaConfig->Deserialize(any, 0, resource);

    _docInfoExtractorFactory = std::make_shared<indexlibv2::plain::DocumentInfoExtractorFactory>();
    _rootPath = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;

    auto loadStrategy =
        std::make_shared<indexlib::file_system::MmapLoadStrategy>(/*lock*/ true, false, 4 * 1024 * 1024, 0);
    indexlib::file_system::LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");

    indexlib::file_system::LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetFilePatternString(pattern);
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetName("__test__");

    indexlib::file_system::LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);

    fsOptions.loadConfigList = loadConfigList;
    auto fs = indexlib::file_system::FileSystemCreator::Create("meta", _rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);
    _metaDir = rootDir->MakeDirectory(FIELD_META_DIR_NAME);
}

void FieldMetaDiskIndexerTest::tearDown() {}

std::shared_ptr<FieldMetaMemIndexer>
FieldMetaDiskIndexerTest::CreateMemIndexer(const std::shared_ptr<FieldMetaConfig>& metaConfig)
{
    auto memIndexer = std::make_shared<FieldMetaMemIndexer>();
    EXPECT_TRUE(memIndexer->Init(metaConfig, _docInfoExtractorFactory.get()).IsOK());
    return memIndexer;
}

void FieldMetaDiskIndexerTest::prepareData(const std::shared_ptr<FieldMetaConfig>& metaConfig)
{
    auto memIndexer = CreateMemIndexer(metaConfig);
    ASSERT_TRUE(memIndexer);
    autil::mem_pool::Pool pool;
    for (int i = 1; i <= 1000; i++) {
        std::string data = std::to_string(i);
        auto batch = CreateFieldMetaValueBatch(i, data, 4, /*isNull*/ false);
        ASSERT_TRUE(memIndexer->_sourceWriter->Build(batch).IsOK());
        for (auto& meta : memIndexer->_fieldMetas) {
            ASSERT_TRUE(meta->Build(batch).IsOK());
        }
    }
    memIndexer->_isDirty = true;

    util::SimplePool dumpPool;
    ASSERT_TRUE(memIndexer->Dump(&dumpPool, _metaDir, nullptr).IsOK());
    _indexerParam.docCount = 1000;
}

IFieldMeta::FieldValueBatch FieldMetaDiskIndexerTest::CreateFieldMetaValueBatch(int64_t key, const std::string& value,
                                                                                uint32_t fieldTokenCount, bool isNull)
{
    FieldValueMeta fieldValueMeta {value, fieldTokenCount};
    docid_t docId = key;
    return IFieldMeta::FieldValueBatch {{fieldValueMeta, isNull, docId}};
}

TEST_F(FieldMetaDiskIndexerTest, testRead)
{
    prepareData(_metaConfig);
    auto diskIndexer = std::make_shared<FieldMetaDiskIndexer>(_indexerParam);
    ASSERT_TRUE(diskIndexer->Open(_metaConfig, _metaDir->GetIDirectory()).IsOK());

    size_t fieldTokenCount;
    autil::mem_pool::Pool pool;
    ASSERT_FALSE(diskIndexer->GetFieldTokenCount(1, &pool, fieldTokenCount));

    auto meta = diskIndexer->GetSegmentFieldMeta("MinMax");
    auto minMaxMeta = std::dynamic_pointer_cast<MinMaxFieldMeta<int32_t>>(meta);
    ASSERT_TRUE(minMaxMeta);
    ASSERT_EQ(1000, minMaxMeta->GetMaxValue());
    ASSERT_EQ(1, minMaxMeta->GetMinValue());
}

TEST_F(FieldMetaDiskIndexerTest, testEstimateMemUseNotTextField)
{
    // ft_int not support field lenght, not open, mem use is 0
    prepareData(_metaConfig);
    auto diskIndexer = std::make_shared<FieldMetaDiskIndexer>(_indexerParam);
    size_t estimateMem = diskIndexer->EstimateMemUsed(_metaConfig, _metaDir->GetIDirectory());
    ASSERT_EQ(0, estimateMem);

    ASSERT_TRUE(diskIndexer->Open(_metaConfig, _metaDir->GetIDirectory()).IsOK());
    size_t evaluateMem = diskIndexer->EvaluateCurrentMemUsed();
    ASSERT_EQ(0, evaluateMem);
}

TEST_F(FieldMetaDiskIndexerTest, testMemUseAndReadForTextField)
{
    // test text field
    std::string jsonStr = R"({
            "field_name" : "title",
            "index_name" : "title",
            "metas" : [ { "metaName" : "FieldTokenCount" } ]
    })";

    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("title", ft_text, /*isMulti*/ false);
    indexlibv2::config::MutableJson runtimeSettings;
    indexlibv2::config::IndexConfigDeserializeResource resource({fieldConfig}, runtimeSettings);
    auto metaConfig = std::make_shared<FieldMetaConfig>();
    autil::legacy::Any any = autil::legacy::json::ParseJson(jsonStr);
    metaConfig->Deserialize(any, 0, resource);

    auto memIndexer = CreateMemIndexer(metaConfig);
    ASSERT_TRUE(memIndexer);
    autil::mem_pool::Pool pool;
    for (int i = 1; i <= 1000; i++) {
        std::string data(i, 'a');
        auto batch = CreateFieldMetaValueBatch(i, data, i, /*isNull*/ false);
        ASSERT_TRUE(memIndexer->_sourceWriter->Build(batch).IsOK());
        for (auto& meta : memIndexer->_fieldMetas) {
            ASSERT_TRUE(meta->Build(batch).IsOK());
        }
    }
    memIndexer->_isDirty = true;

    util::SimplePool dumpPool;
    ASSERT_TRUE(memIndexer->Dump(&dumpPool, _metaDir, nullptr).IsOK());
    _indexerParam.docCount = 1000;

    auto diskIndexer = std::make_shared<FieldMetaDiskIndexer>(_indexerParam);
    size_t estimateMem = diskIndexer->EstimateMemUsed(metaConfig, _metaDir->GetIDirectory());
    ASSERT_GT(estimateMem, 0);

    ASSERT_TRUE(diskIndexer->Open(metaConfig, _metaDir->GetIDirectory()).IsOK());
    size_t evaluateMem = diskIndexer->EvaluateCurrentMemUsed();
    ASSERT_EQ(estimateMem, evaluateMem);

    uint64_t count;
    for (int i = 1; i <= 1000; i++) {
        ASSERT_TRUE(diskIndexer->GetFieldTokenCount(i - 1, &pool, count));
        ASSERT_EQ(i, count);
    }

    auto meta = diskIndexer->GetSegmentFieldMeta("FieldTokenCount");
    std::shared_ptr<FieldTokenCountMeta> fieldTokenCountMeta = std::dynamic_pointer_cast<FieldTokenCountMeta>(meta);
    ASSERT_TRUE(fieldTokenCountMeta);
    ASSERT_EQ(1000, fieldTokenCountMeta->_docCount);
    ASSERT_EQ(500500, fieldTokenCountMeta->_totalTokenCount);
    ASSERT_DOUBLE_EQ(500.5, fieldTokenCountMeta->GetAvgFieldTokenCount());
}

} // namespace indexlib::index
