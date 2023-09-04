#include "indexlib/index/attribute/SingleValueAttributeDiskIndexer.h"

#include "autil/ConstString.h"
#include "autil/LongHashValue.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "fslib/fs/File.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/index/attribute/SingleValueAttributeMemIndexer.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "unittest/unittest.h"

using namespace indexlibv2::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlibv2::index {

class SingleValueAttributeDiskIndexerTest : public TESTBASE
{
public:
    // for sort pattern
    enum class ValueMode { VM_NORMAL, VM_ASC, VM_DESC };
    enum class SearchType { ST_LOWERBOUND, ST_UPPERBOUND };

public:
    SingleValueAttributeDiskIndexerTest() = default;
    ~SingleValueAttributeDiskIndexerTest() = default;

public:
    void setUp() override;

private:
    template <typename T>
    void InnerTestOpenAndRead();

    template <typename T>
    void InnerTestOpenAndSearch(bool compress, bool needNull);

    template <typename T>
    void InnerTestMemUsed(bool compress, bool needNull);

    template <typename T>
    void CheckRead(const std::vector<T>& expectedData, const std::shared_ptr<AttributeConfig>& attrConfig,
                   const std::shared_ptr<indexlib::file_system::Directory>& attrDir);

    template <typename T, typename Comp>
    void CheckSearch(const std::vector<T>& expectedData, const std::shared_ptr<AttributeConfig>& attrConfig,
                     const std::shared_ptr<indexlib::file_system::Directory>& attrDir, ValueMode mode, SearchType type);

    template <typename T>
    void MakeOneAttribute(std::vector<T>& fieldValues, ValueMode valueMode, bool needCompress, bool needNull,
                          std::optional<T> defaultValue = std::nullopt);

    template <typename T>
    std::unique_ptr<SingleValueAttributeMemIndexer<T>>
    PrepareMemIndexer(const std::shared_ptr<AttributeConfig>& attrConfig);

    template <typename T>
    T GetValueByMode(uint32_t i, ValueMode mode);

private:
    std::shared_ptr<AttributeConfig> _attrConfig;
    std::shared_ptr<Directory> _attrDir;
    std::shared_ptr<document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
    IndexerParameter _indexerParam;
    std::string _rootPath;

    static const uint32_t DOC_NUM = 100;
};

void SingleValueAttributeDiskIndexerTest::setUp()
{
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
    loadConfig.SetName("__OFFLINE_ATTRIBUTE__");

    indexlib::file_system::LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);

    fsOptions.loadConfigList = loadConfigList;
    _rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", _rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);
    _attrDir = rootDir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    ASSERT_TRUE(_attrDir != nullptr);
    _docInfoExtractorFactory = std::make_shared<plain::DocumentInfoExtractorFactory>();
}

template <typename T>
void SingleValueAttributeDiskIndexerTest::MakeOneAttribute(std::vector<T>& fieldValues, ValueMode valueMode,
                                                           bool needCompress, bool needNull,
                                                           std::optional<T> defaultValue)
{
    tearDown();
    setUp();
    _indexerParam = IndexerParameter {};
    _attrConfig = AttributeTestUtil::CreateAttrConfig<T>(/*isMultiValue*/ false,
                                                         /*compressType*/ std::nullopt);
    if (needCompress) {
        ASSERT_TRUE(_attrConfig->SetCompressType("equal").IsOK());
    }
    _attrConfig->GetFieldConfig()->SetEnableNullField(needNull);
    if (defaultValue != std::nullopt) {
        _attrConfig->GetFieldConfig()->SetDefaultValue(std::to_string(defaultValue.value()));
        fieldValues = std::vector<T>(DOC_NUM, defaultValue.value());
        _indexerParam.docCount = DOC_NUM;
        _indexerParam.readerOpenType = IndexerParameter::READER_DEFAULT_VALUE;
        return;
    }

    auto memIndexer = PrepareMemIndexer<T>(_attrConfig);
    ASSERT_TRUE(memIndexer != nullptr);

    autil::mem_pool::Pool pool;
    docid_t lastDocId = 0;
    fieldValues.clear();
    _indexerParam.docCount = 0;
    if (needNull && valueMode == ValueMode::VM_ASC) {
        // add some null field at the begin
        for (int32_t i = 0; i < 10; i++) {
            memIndexer->AddField(i, autil::StringView::empty_instance(), true);
        }
        lastDocId = 10;
        _indexerParam.docCount += 10;
    }
    for (uint32_t i = 0; i < DOC_NUM; ++i) {
        T value = GetValueByMode<T>(i, valueMode);
        fieldValues.push_back(value);

        char buf[64];
        snprintf(buf, sizeof(buf), "%d", (int32_t)value);
        autil::StringView encodeValue = memIndexer->_attrConvertor->Encode(autil::StringView(buf), &pool);
        memIndexer->AddField((docid_t)i + lastDocId, encodeValue, false);
    }
    _indexerParam.docCount += DOC_NUM;
    if (needNull && valueMode == ValueMode::VM_DESC) {
        // add some null field at the last
        for (uint32_t i = 0; i < 10; i++) {
            memIndexer->AddField((docid_t)(i + DOC_NUM), autil::StringView::empty_instance(), true);
        }
        _indexerParam.docCount += 10;
    }

    SimplePool dumpPool;
    ASSERT_TRUE(memIndexer->Dump(&dumpPool, _attrDir, nullptr).IsOK());
}

template <typename T>
std::unique_ptr<SingleValueAttributeMemIndexer<T>>
SingleValueAttributeDiskIndexerTest::PrepareMemIndexer(const std::shared_ptr<AttributeConfig>& attrConfig)
{
    auto memIndexer = std::make_unique<SingleValueAttributeMemIndexer<T>>(_indexerParam);
    if (memIndexer->Init(attrConfig, _docInfoExtractorFactory.get()).IsOK()) {
        return memIndexer;
    }
    // init failed
    return nullptr;
}

template <typename T>
void SingleValueAttributeDiskIndexerTest::CheckRead(const std::vector<T>& expectedData,
                                                    const std::shared_ptr<AttributeConfig>& attrConfig,
                                                    const std::shared_ptr<indexlib::file_system::Directory>& attrDir)
{
    auto diskIndexer = std::make_unique<SingleValueAttributeDiskIndexer<T>>(/*metrics*/ nullptr, _indexerParam);
    ASSERT_TRUE(diskIndexer->Open(attrConfig, attrDir->GetIDirectory()).IsOK());

    size_t i = 0;
    for (size_t j = 0; j < _indexerParam.docCount; j++) {
        T value;
        bool isNull = false;
        auto ctx = diskIndexer->CreateReadContext(nullptr);
        diskIndexer->Read(j, value, isNull, ctx);
        if (isNull) {
            continue;
        }
        ASSERT_EQ(expectedData[i], value);
        i++;
    }
    ASSERT_EQ(expectedData.size(), i);
}

template <typename T, typename Comp>
void SingleValueAttributeDiskIndexerTest::CheckSearch(const std::vector<T>& expectedData,
                                                      const std::shared_ptr<AttributeConfig>& attrConfig,
                                                      const std::shared_ptr<indexlib::file_system::Directory>& attrDir,
                                                      ValueMode mode, SearchType type)
{
    auto diskIndexer = std::make_unique<SingleValueAttributeDiskIndexer<T>>(/*metrics*/ nullptr, _indexerParam);
    ASSERT_TRUE(diskIndexer->Open(attrConfig, attrDir->GetIDirectory()).IsOK());

    const config::SortPattern sp = mode == ValueMode::VM_DESC ? config::sp_desc : config::sp_asc;
    docid_t docCount = diskIndexer->TEST_GetDocCount();
    docid_t nullCount = docCount - expectedData.size();
    DocIdRange rangeLimit(0, docCount);
    ASSERT_EQ(nullCount, diskIndexer->SearchNullCount(sp));

    for (size_t i = 0; i < expectedData.size(); ++i) {
        T value = expectedData[i];
        docid_t docid = INVALID_DOCID;
        ASSERT_TRUE(diskIndexer->template Search<Comp>(value, rangeLimit, sp, docid).IsOK());
        if (type == SearchType::ST_UPPERBOUND) {
            docid_t expectedDocid = sp == config::sp_asc ? (docid_t)i / 2 * 2 + 2 + nullCount : (docid_t)i / 2 * 2 + 2;
            ASSERT_EQ(expectedDocid, docid);
        }
        if (type == SearchType::ST_LOWERBOUND) {
            docid_t expectedDocid = sp == config::sp_asc ? (docid_t)i / 2 * 2 + nullCount : (docid_t)i / 2 * 2;
            ASSERT_EQ(expectedDocid, docid);
        }
    }
}

template <typename T>
T SingleValueAttributeDiskIndexerTest::GetValueByMode(uint32_t i, ValueMode mode)
{
    T value;
    switch (mode) {
    case ValueMode::VM_ASC:
        value = i / 2 * 2;
        break;
    case ValueMode::VM_DESC:
        value = (DOC_NUM - i - 1) / 2 * 2;
        break;
    case ValueMode::VM_NORMAL:
    default:
        value = i * 3 % 10;
        break;
    }
    return value;
}

template <typename T>
void SingleValueAttributeDiskIndexerTest::InnerTestOpenAndRead()
{
    std::vector<T> expectedData;
    MakeOneAttribute(expectedData, ValueMode::VM_NORMAL, false, false);
    CheckRead(expectedData, _attrConfig, _attrDir);

    MakeOneAttribute<T>(expectedData, ValueMode::VM_NORMAL, false, false, 1);
    CheckRead(expectedData, _attrConfig, _attrDir);
}

template <typename T>
void SingleValueAttributeDiskIndexerTest::InnerTestOpenAndSearch(bool compress, bool needNull)
{
    std::vector<T> expectedData;
    MakeOneAttribute(expectedData, ValueMode::VM_DESC, compress, needNull);
    CheckSearch<T, std::less<T>>(expectedData, _attrConfig, _attrDir, ValueMode::VM_DESC, SearchType::ST_LOWERBOUND);
    MakeOneAttribute(expectedData, ValueMode::VM_DESC, compress, needNull);
    CheckSearch<T, std::less_equal<T>>(expectedData, _attrConfig, _attrDir, ValueMode::VM_DESC,
                                       SearchType::ST_UPPERBOUND);
    MakeOneAttribute(expectedData, ValueMode::VM_ASC, compress, needNull);
    CheckSearch<T, std::greater<T>>(expectedData, _attrConfig, _attrDir, ValueMode::VM_ASC, SearchType::ST_LOWERBOUND);
    MakeOneAttribute(expectedData, ValueMode::VM_ASC, compress, needNull);
    CheckSearch<T, std::greater_equal<T>>(expectedData, _attrConfig, _attrDir, ValueMode::VM_ASC,
                                          SearchType::ST_UPPERBOUND);
}

template <typename T>
void SingleValueAttributeDiskIndexerTest::InnerTestMemUsed(bool compress, bool needNull)
{
    std::vector<T> expectedData;
    MakeOneAttribute(expectedData, ValueMode::VM_DESC, compress, needNull);
    auto diskIndexer = std::make_unique<SingleValueAttributeDiskIndexer<T>>(/*metrics*/ nullptr, _indexerParam);
    auto estimateMemUsed = diskIndexer->EstimateMemUsed(_attrConfig, _attrDir->GetIDirectory());
    ASSERT_TRUE(diskIndexer->Open(_attrConfig, _attrDir->GetIDirectory()).IsOK());
    auto evaluateMemUsed = diskIndexer->EvaluateCurrentMemUsed();

    ASSERT_TRUE(estimateMemUsed > 0);
    ASSERT_EQ(estimateMemUsed, evaluateMemUsed);
}

TEST_F(SingleValueAttributeDiskIndexerTest, TestIsInMemory)
{
    std::shared_ptr<AttributeConfig> attrConfig =
        AttributeTestUtil::CreateAttrConfig<int32_t>(/*isMultiValue*/ false, /*compressType*/ std::nullopt);
    SingleValueAttributeDiskIndexer<int32_t> int32Reader(nullptr, _indexerParam);
    ASSERT_EQ(false, int32Reader.IsInMemory());
}

TEST_F(SingleValueAttributeDiskIndexerTest, TestGetDataLength)
{
    std::shared_ptr<AttributeConfig> attrConfig =
        AttributeTestUtil::CreateAttrConfig<int32_t>(/*isMultiValue*/ false, /*compressType*/ std::nullopt);
    SingleValueAttributeDiskIndexer<int32_t> int32Reader(nullptr, _indexerParam);
    ASSERT_EQ(sizeof(int32_t), int32Reader.TEST_GetDataLength(0, nullptr));
    ASSERT_EQ(sizeof(int32_t), int32Reader.TEST_GetDataLength(100, nullptr));

    attrConfig = AttributeTestUtil::CreateAttrConfig<int8_t>(/*isMultiValue*/ false, /*compressType*/ std::nullopt);
    SingleValueAttributeDiskIndexer<int8_t> int8Reader(nullptr, _indexerParam);
    ASSERT_EQ(sizeof(int8_t), int8Reader.TEST_GetDataLength(0, nullptr));
    ASSERT_EQ(sizeof(int8_t), int8Reader.TEST_GetDataLength(100, nullptr));
}

TEST_F(SingleValueAttributeDiskIndexerTest, TestCaseForRead)
{
    InnerTestOpenAndRead<uint32_t>();
    InnerTestOpenAndRead<int32_t>();
    InnerTestOpenAndRead<uint64_t>();
    InnerTestOpenAndRead<int64_t>();
    InnerTestOpenAndRead<uint8_t>();
    InnerTestOpenAndRead<int8_t>();
    InnerTestOpenAndRead<uint16_t>();
    InnerTestOpenAndRead<int16_t>();
}

TEST_F(SingleValueAttributeDiskIndexerTest, TestCaseForSearch)
{
    InnerTestOpenAndSearch<int8_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestOpenAndSearch<uint8_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestOpenAndSearch<int16_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestOpenAndSearch<uint16_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestOpenAndSearch<int32_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestOpenAndSearch<uint32_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestOpenAndSearch<int64_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestOpenAndSearch<uint64_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestOpenAndSearch<float>(/*compress*/ false, /*needNull*/ false);
    InnerTestOpenAndSearch<double>(/*compress*/ false, /*needNull*/ false);
}

TEST_F(SingleValueAttributeDiskIndexerTest, TestCaseForSearchWithCompress)
{
    // test for compress
    InnerTestOpenAndSearch<int8_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestOpenAndSearch<uint8_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestOpenAndSearch<int16_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestOpenAndSearch<uint16_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestOpenAndSearch<int32_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestOpenAndSearch<uint32_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestOpenAndSearch<int64_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestOpenAndSearch<uint64_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestOpenAndSearch<float>(/*compress*/ true, /*needNull*/ false);
    InnerTestOpenAndSearch<double>(/*compress*/ true, /*needNull*/ false);
}

TEST_F(SingleValueAttributeDiskIndexerTest, TestCaseForSearchWithNull)
{
    // test for null field
    InnerTestOpenAndSearch<int8_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestOpenAndSearch<uint8_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestOpenAndSearch<int16_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestOpenAndSearch<uint16_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestOpenAndSearch<int32_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestOpenAndSearch<uint32_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestOpenAndSearch<int64_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestOpenAndSearch<uint64_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestOpenAndSearch<float>(/*compress*/ false, /*needNull*/ true);
    InnerTestOpenAndSearch<double>(/*compress*/ false, /*needNull*/ true);
}

TEST_F(SingleValueAttributeDiskIndexerTest, TestCaseForMemUsed)
{
    InnerTestMemUsed<int8_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestMemUsed<uint8_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestMemUsed<int16_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestMemUsed<uint16_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestMemUsed<int32_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestMemUsed<uint32_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestMemUsed<int64_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestMemUsed<uint64_t>(/*compress*/ false, /*needNull*/ true);
    InnerTestMemUsed<float>(/*compress*/ false, /*needNull*/ true);
    InnerTestMemUsed<double>(/*compress*/ false, /*needNull*/ true);

    InnerTestMemUsed<int8_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestMemUsed<uint8_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestMemUsed<int16_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestMemUsed<uint16_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestMemUsed<int32_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestMemUsed<uint32_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestMemUsed<int64_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestMemUsed<uint64_t>(/*compress*/ false, /*needNull*/ false);
    InnerTestMemUsed<float>(/*compress*/ false, /*needNull*/ false);
    InnerTestMemUsed<double>(/*compress*/ false, /*needNull*/ false);

    InnerTestMemUsed<int8_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestMemUsed<uint8_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestMemUsed<int16_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestMemUsed<uint16_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestMemUsed<int32_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestMemUsed<uint32_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestMemUsed<int64_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestMemUsed<uint64_t>(/*compress*/ true, /*needNull*/ false);
    InnerTestMemUsed<float>(/*compress*/ true, /*needNull*/ false);
    InnerTestMemUsed<double>(/*compress*/ true, /*needNull*/ false);
}

} // namespace indexlibv2::index
