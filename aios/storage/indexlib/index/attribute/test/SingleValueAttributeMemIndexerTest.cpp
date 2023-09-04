#include "indexlib/index/attribute/SingleValueAttributeMemIndexer.h"

#include <random>

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/SingleFileCompressConfig.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MemDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/attribute/SingleValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "unittest/unittest.h"
using namespace std;

using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlibv2::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlibv2::index {
class SingleValueAttributeMemIndexerTest : public TESTBASE
{
public:
    SingleValueAttributeMemIndexerTest() = default;
    ~SingleValueAttributeMemIndexerTest() = default;

    void setUp() override;
    void tearDown() override {}

private:
    template <typename T>
    void AddData(SingleValueAttributeMemIndexer<T>& writer, T expectedData[], uint32_t docNum,
                 const std::shared_ptr<AttributeConvertor>& convertor);

    template <typename T>
    void UpdateData(SingleValueAttributeMemIndexer<T>& writer, T expectedData[], uint32_t docNum,
                    const std::shared_ptr<AttributeConvertor>& convertor);

    template <typename T>
    void TestAddField();

    template <typename T>
    void TestUpdateField();

    template <typename T>
    void CheckWriterData(const SingleValueAttributeMemIndexer<T>& writer, T expectedData[], uint32_t docNum);

    void CheckDataFile(uint32_t docNum, const std::string& dataFilePath, size_t typeSize);

private:
    bool _alreadRegister = false;
    std::string _dir;
    std::shared_ptr<document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
    std::unique_ptr<indexlib::util::BuildResourceMetrics> _buildResourceMetrics;
    indexlib::util::BuildResourceMetricsNode* _buildResourceMetricsNode;
    std::shared_ptr<index::BuildingIndexMemoryUseUpdater> _memoryUseUpdater;
};

void SingleValueAttributeMemIndexerTest::setUp()
{
    _dir = GET_TEMP_DATA_PATH();
    _docInfoExtractorFactory = std::make_shared<indexlibv2::plain::DocumentInfoExtractorFactory>();

    if (!_alreadRegister) {
        _buildResourceMetrics = std::make_unique<indexlib::util::BuildResourceMetrics>();
        _buildResourceMetrics->Init();
        _alreadRegister = true;
    }
    _buildResourceMetricsNode = _buildResourceMetrics->AllocateNode();
    _memoryUseUpdater = std::make_shared<index::BuildingIndexMemoryUseUpdater>(_buildResourceMetricsNode);
}

template <typename T>
void SingleValueAttributeMemIndexerTest::AddData(SingleValueAttributeMemIndexer<T>& writer, T expectedData[],
                                                 uint32_t docNum, const std::shared_ptr<AttributeConvertor>& convertor)
{
    autil::mem_pool::Pool pool;
    for (uint32_t i = 0; i < (docNum - 1); ++i) {
        T value = i * 3 % 10;
        expectedData[i] = value;
        string fieldValue = StringUtil::toString(value);
        StringView encodeStr = convertor->Encode(StringView(fieldValue), &pool);
        writer.AddField((docid_t)i, encodeStr, false);
    }

    writer.UpdateMemUse(_memoryUseUpdater.get());

    EXPECT_EQ((docNum - 1) * sizeof(T), _buildResourceMetricsNode->GetValue(BMT_DUMP_FILE_SIZE));
    // EXPECT_EQ(, _buildResourceMetricsNode->GetValue(BMT_CURRENT_MEMORY_USE));
    EXPECT_EQ(0, _buildResourceMetricsNode->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
    EXPECT_EQ(0, _buildResourceMetricsNode->GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE));

    // last doc for empty value
    expectedData[docNum - 1] = 0;
    writer.AddField((docid_t)(docNum - 1), StringView(), false);
    writer.UpdateMemUse(_memoryUseUpdater.get());
}

template <typename T>
void SingleValueAttributeMemIndexerTest::UpdateData(SingleValueAttributeMemIndexer<T>& writer, T expectedData[],
                                                    uint32_t docNum,
                                                    const std::shared_ptr<AttributeConvertor>& convertor)
{
    int64_t totalMemUseBefore = _buildResourceMetricsNode->GetValue(BMT_CURRENT_MEMORY_USE);
    int64_t dumpTempMemUseBefore = _buildResourceMetricsNode->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE);
    int64_t dumpTempExpandMemUseBefore = _buildResourceMetricsNode->GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE);
    int64_t dumpFileSizeBefore = _buildResourceMetricsNode->GetValue(BMT_DUMP_FILE_SIZE);
    autil::mem_pool::Pool pool;
    for (uint32_t i = 0; i < docNum; ++i) {
        if (i % 5 == 0) {
            T value = i * 3 % 12;
            expectedData[i] = value;

            string fieldValue = StringUtil::toString(value);
            StringView encodeStr = convertor->Encode(StringView(fieldValue), &pool);
            writer.TEST_UpdateField((docid_t)i, encodeStr, false);
        }
    }
    writer.UpdateMemUse(_memoryUseUpdater.get());
    EXPECT_EQ(totalMemUseBefore, _buildResourceMetricsNode->GetValue(BMT_CURRENT_MEMORY_USE));
    EXPECT_EQ(dumpTempMemUseBefore, _buildResourceMetricsNode->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
    EXPECT_EQ(dumpTempExpandMemUseBefore, _buildResourceMetricsNode->GetValue(BMT_DUMP_EXPAND_MEMORY_SIZE));
    EXPECT_EQ(dumpFileSizeBefore, _buildResourceMetricsNode->GetValue(BMT_DUMP_FILE_SIZE));
}

void SingleValueAttributeMemIndexerTest::CheckDataFile(uint32_t docNum, const string& dataFilePath, size_t typeSize)
{
    FileMeta fileMeta;
    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(_dir + dataFilePath, fileMeta));
    ASSERT_EQ((int64_t)typeSize * docNum, fileMeta.fileLength);
}

template <typename T>
void SingleValueAttributeMemIndexerTest::TestAddField()
{
    std::shared_ptr<AttributeConfig> attrConfig = AttributeTestUtil::CreateAttrConfig<T>(false, std::nullopt);
    IndexerParameter indexerParameter;
    SingleValueAttributeMemIndexer<T> writer(indexerParameter);
    ASSERT_TRUE(writer.Init(attrConfig, _docInfoExtractorFactory.get()).IsOK());

    static const uint32_t DOC_NUM = 100;
    T expectedData[DOC_NUM];
    AddData(writer, expectedData, DOC_NUM, writer._attrConvertor);
    CheckWriterData(writer, expectedData, DOC_NUM);

    std::shared_ptr<Directory> outputDir;
    AttributeTestUtil::DumpWriter(writer, _dir, nullptr, outputDir);
    // check data file
    std::string dataFilePath = attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
    CheckDataFile(DOC_NUM, dataFilePath, sizeof(T));
}

template <typename T>
void SingleValueAttributeMemIndexerTest::TestUpdateField()
{
    std::shared_ptr<AttributeConfig> attrConfig = AttributeTestUtil::CreateAttrConfig<T>(false, std::nullopt);
    IndexerParameter indexerParameter;
    SingleValueAttributeMemIndexer<T> writer(indexerParameter);
    ASSERT_TRUE(writer.Init(attrConfig, _docInfoExtractorFactory.get()).IsOK());

    static const uint32_t DOC_NUM = 100;
    T expectedData[DOC_NUM];
    AddData(writer, expectedData, DOC_NUM, writer._attrConvertor);
    UpdateData(writer, expectedData, DOC_NUM, writer._attrConvertor);
    CheckWriterData(writer, expectedData, DOC_NUM);
}

template <typename T>
void SingleValueAttributeMemIndexerTest::CheckWriterData(const SingleValueAttributeMemIndexer<T>& writer,
                                                         T expectedData[], uint32_t docNum)
{
    indexlib::index::TypedSliceList<T>* data =
        static_cast<indexlib::index::TypedSliceList<T>*>(writer._formatter->_data);
    for (uint32_t i = 0; i < docNum; ++i) {
        // assert(expectedData[i] == (*sliceArray)[i]);
        T value;
        data->Read(i, value);
        ASSERT_EQ(expectedData[i], value);
    }
}

TEST_F(SingleValueAttributeMemIndexerTest, TestAddField4UInt32) { TestAddField<uint32_t>(); }
TEST_F(SingleValueAttributeMemIndexerTest, TestUpdateField4UInt32) { TestUpdateField<uint32_t>(); }
TEST_F(SingleValueAttributeMemIndexerTest, TestAddField4Float) { TestAddField<float>(); }
TEST_F(SingleValueAttributeMemIndexerTest, TestUpdateField4Float) { TestUpdateField<float>(); }
TEST_F(SingleValueAttributeMemIndexerTest, TestSimpleDump)
{
    std::shared_ptr<AttributeConfig> attrConfig = AttributeTestUtil::CreateAttrConfig<uint32_t>(false, std::nullopt);

    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    autil::mem_pool::Pool pool;
    IndexerParameter indexerParameter;
    SingleValueAttributeMemIndexer<uint32_t> writer(indexerParameter);
    ASSERT_TRUE(writer.Init(attrConfig, _docInfoExtractorFactory.get()).IsOK());

    for (uint32_t i = 0; i < 500000; ++i) {
        uint32_t value = i * 3 % 10;
        string fieldValue = StringUtil::toString(value);
        StringView encodeStr = convertor->Encode(StringView(fieldValue), &pool);
        writer.AddField((docid_t)i, encodeStr, false);
    }

    string dirPath = GET_TEMP_DATA_PATH() + "dump_path";
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;

    auto fileSystem = FileSystemCreator::Create("ut_perf_test", dirPath, fsOptions).GetOrThrow();
    auto dir = indexlib::file_system::Directory::Get(fileSystem);

    SimplePool dumpPool;
    ASSERT_TRUE(writer.Dump(&dumpPool, dir, nullptr).IsOK());
}

TEST_F(SingleValueAttributeMemIndexerTest, TestSortDump)
{
    using T = uint32_t;
    std::shared_ptr<AttributeConfig> attrConfig = AttributeTestUtil::CreateAttrConfig<T>(false, std::nullopt);
    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    autil::mem_pool::Pool pool;
    IndexerParameter indexerParameter;
    SingleValueAttributeMemIndexer<T> writer(indexerParameter);
    ASSERT_TRUE(writer.Init(attrConfig, _docInfoExtractorFactory.get()).IsOK());

    docid_t docCount = 500000;
    auto params = std::make_shared<DocMapDumpParams>();

    for (docid_t i = 0; i < docCount; ++i) {
        T value = i * 3 % 100;
        string fieldValue = StringUtil::toString(value);
        StringView encodeStr = convertor->Encode(StringView(fieldValue), &pool);
        writer.AddField(i, encodeStr, false);
        params->new2old.push_back(i);
    }

    std::random_device rd;
    std::mt19937 rng(rd());
    std::shuffle(params->new2old.begin(), params->new2old.end(), rng);

    string dirPath = GET_TEMP_DATA_PATH() + "dump_path";
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;

    auto fileSystem = FileSystemCreator::Create("sort_dump", dirPath, fsOptions).GetOrThrow();
    auto dir = indexlib::file_system::Directory::Get(fileSystem);
    SimplePool dumpPool;
    ASSERT_TRUE(writer.Dump(&dumpPool, dir, params).IsOK());

    indexerParameter.docCount = docCount;
    auto indexer = std::make_shared<SingleValueAttributeDiskIndexer<T>>(nullptr, indexerParameter);
    ASSERT_TRUE(indexer->Open(attrConfig, dir->GetIDirectory()).IsOK());
    auto readCtx = indexer->CreateReadContext(nullptr);
    for (docid_t i = 0; i < docCount; ++i) {
        docid_t oldDocId = params->new2old[i];
        T value = oldDocId * 3 % 100;
        T result = -1;
        bool isNull = false;
        ASSERT_TRUE(indexer->Read(i, result, isNull, readCtx));
        ASSERT_FALSE(isNull);
        ASSERT_EQ(value, result);
    }
}

TEST_F(SingleValueAttributeMemIndexerTest, TestAddNullField)
{
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<uint32_t>(false, std::nullopt);
    attrConfig->GetFieldConfig()->SetEnableNullField(true);

    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    autil::mem_pool::Pool pool;
    IndexerParameter indexerParameter;
    SingleValueAttributeMemIndexer<uint32_t> writer(indexerParameter);
    ASSERT_TRUE(writer.Init(attrConfig, _docInfoExtractorFactory.get()).IsOK());

    string fieldValue = StringUtil::toString(0);
    StringView encodeStr = convertor->Encode(StringView(fieldValue), &pool);
    bool isNull = false;
    for (int i = 0; i < 200; i++) {
        if (i % 3 == 0) {
            isNull = true;
        } else {
            isNull = false;
        }
        writer.AddField(i, encodeStr, isNull);
    }
    ASSERT_EQ(4, writer._formatter->_nullFieldBitmap->Size());
    uint64_t value1, value2;
    writer._formatter->_nullFieldBitmap->Read(0, value1);
    writer._formatter->_nullFieldBitmap->Read(1, value2);
    ASSERT_TRUE(value1 & (1 << 0));
    ASSERT_FALSE(value1 & (1 << 1));
    ASSERT_TRUE(value1 & (1UL << 63));
    ASSERT_FALSE(value2 & (1 << 0));
    ASSERT_FALSE(value2 & (1 << 1));

    writer.TEST_UpdateField(0, encodeStr, false);
    writer.TEST_UpdateField(63, encodeStr, false);
    writer.TEST_UpdateField(64, encodeStr, true);
    writer.TEST_UpdateField(65, encodeStr, true);
    writer._formatter->_nullFieldBitmap->Read(0, value1);
    writer._formatter->_nullFieldBitmap->Read(1, value2);
    ASSERT_FALSE(value1 & (1 << 0));
    ASSERT_FALSE(value1 & (1UL << 63));
    ASSERT_TRUE(value2 & (1UL << 0));
    ASSERT_TRUE(value2 & (1UL << 1));

    const std::string rootDir = GET_TEMP_DATA_PATH();
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string fenceRoot = indexlib::util::PathUtil::JoinPath(rootDir, "fence");
    std::shared_ptr<IFileSystem> fileSystem =
        indexlib::file_system::FileSystemCreator::Create(/*name*/ "", fenceRoot, fsOptions).GetOrThrow();
    std::shared_ptr<Directory> dir = IDirectory::ToLegacyDirectory(make_shared<MemDirectory>("dump_path", fileSystem));
    SimplePool dumpPool;
    ASSERT_TRUE(writer.Dump(&dumpPool, dir, nullptr).IsOK());
    auto fileReader = dir->CreateFileReader("attr_name/data", FSOpenType::FSOT_BUFFERED);
    uint64_t readValue;
    ASSERT_EQ(FSEC_OK, fileReader->Read((void*)&readValue, sizeof(uint64_t), 0).Code());
    ASSERT_EQ(value1, readValue);
}

TEST_F(SingleValueAttributeMemIndexerTest, TestCompress)
{
    std::shared_ptr<AttributeConfig> attrConfig = AttributeTestUtil::CreateAttrConfig<uint32_t>(false, std::nullopt);
    std::shared_ptr<std::vector<SingleFileCompressConfig>> compressConfigs(new std::vector<SingleFileCompressConfig>);
    SingleFileCompressConfig simpleCompressConfig("simple", "zstd");

    compressConfigs->push_back(simpleCompressConfig);
    std::shared_ptr<FileCompressConfigV2> fileCompressConfig(new FileCompressConfigV2(compressConfigs, "simple"));
    attrConfig->SetFileCompressConfigV2(fileCompressConfig);

    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    autil::mem_pool::Pool pool;
    IndexerParameter indexerParameter;
    SingleValueAttributeMemIndexer<uint32_t> writer(indexerParameter);
    ASSERT_TRUE(writer.Init(attrConfig, _docInfoExtractorFactory.get()).IsOK());

    for (uint32_t i = 0; i < 5; ++i) {
        uint32_t value = i;
        string fieldValue = StringUtil::toString(value);
        StringView encodeStr = convertor->Encode(StringView(fieldValue), &pool);
        writer.AddField((docid_t)i, encodeStr, false);
    }

    string dirPath = GET_TEMP_DATA_PATH() + "dump_path";
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;

    auto fileSystem = FileSystemCreator::Create("ut_perf_test", dirPath, fsOptions).GetOrThrow();
    auto dir = indexlib::file_system::Directory::Get(fileSystem);

    SimplePool dumpPool;
    ASSERT_TRUE(writer.Dump(&dumpPool, dir, nullptr).IsOK());
    auto fieldDir = dir->GetDirectory(attrConfig->GetIndexName(), true);
    ASSERT_TRUE(fieldDir->IsExist("data"));
    auto compressInfo = fieldDir->GetCompressFileInfo("data");
    ASSERT_TRUE(compressInfo);
}

} // namespace indexlibv2::index
