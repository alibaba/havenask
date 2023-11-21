#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/MultiValueAttributeMemIndexer.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "indexlib/index/attribute/test/IndexMemoryReclaimerTest.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace indexlib::file_system;
using namespace indexlibv2::config;
using namespace indexlib::util;
using namespace indexlib::index;
using indexlib::config::CompressTypeOption;

namespace indexlibv2::index {

class MultiValueAttributeDiskIndexerTest : public TESTBASE
{
public:
    MultiValueAttributeDiskIndexerTest();
    ~MultiValueAttributeDiskIndexerTest();

public:
    void setUp() override;

public:
    template <typename T>
    void InnerTestRead(uint64_t offsetThreshold, uint64_t offsetUnitSize, bool uniqEncode, bool needEqualCompress);

    template <typename T>
    void InnerTestUpdate(uint64_t offsetThreshold, uint64_t offsetUnitSize, bool uniqEncode, bool needEqualCompress);

    template <typename T>
    void InnerTestMemUsed(uint64_t offsetThreshold, uint64_t offsetUnitSize, bool uniqEncode, bool needEqualCompress);

private:
    template <typename T>
    void DoTestRead(uint64_t offsetThreshold, uint64_t offsetUnitSize, bool uniqEncode, bool needEqualCompress,
                    bool defaultValue);

    template <typename T>
    void MakeOneAttributeSegment(std::vector<std::vector<T>>& expectData, std::shared_ptr<AttributeConfig>& attrConfig,
                                 uint64_t offsetUnitSize, bool needCompress);

    void CheckOffsetFileLength(std::shared_ptr<AttributeConfig>& attrConfig, int64_t targetLength);

    template <typename T>
    void CheckRead(const std::vector<std::vector<T>>& expectedValues, MultiValueAttributeDiskIndexer<T>& diskIndexer);

    template <typename T>
    void MakeUpdateData(std::map<docid_t, std::vector<T>>& toUpdateDocs);

    template <typename T>
    void CheckUpdate(const std::vector<std::vector<T>>& expectedValues, MultiValueAttributeDiskIndexer<T>& diskIndexer);

private:
    std::shared_ptr<mem_pool::Pool> _pool;
    std::shared_ptr<Directory> _attrDir;
    std::shared_ptr<indexlibv2::document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
    DiskIndexerParameter _indexerParam;
    std::string _rootPath;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::shared_ptr<indexlibv2::framework::IIndexMemoryReclaimer> _indexMemoryReclaimer;
    static const uint32_t DOC_NUM = 100;
    static const uint32_t UPDATE_DOC_NUM = 20;
};

MultiValueAttributeDiskIndexerTest::MultiValueAttributeDiskIndexerTest()
{
    _pool = std::make_shared<autil::mem_pool::Pool>();
    _indexMemoryReclaimer = std::make_shared<indexlibv2::index::IndexMemoryReclaimerTest>();
}

MultiValueAttributeDiskIndexerTest::~MultiValueAttributeDiskIndexerTest() {}

void MultiValueAttributeDiskIndexerTest::setUp()
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
    assert(_attrDir != nullptr);
    _docInfoExtractorFactory = std::make_shared<plain::DocumentInfoExtractorFactory>();

    kmonitor::MetricsTags metricsTags;
    _metricsReporter = std::make_shared<kmonitor::MetricsReporter>("", metricsTags, "");
}

template <typename T>
void MultiValueAttributeDiskIndexerTest::MakeOneAttributeSegment(std::vector<std::vector<T>>& expectData,
                                                                 std::shared_ptr<AttributeConfig>& attrConfig,
                                                                 uint64_t offsetUnitSize, bool needCompress)
{
    tearDown();
    setUp();

    _indexerParam.docCount = DOC_NUM;
    _indexerParam.indexMemoryReclaimer = _indexMemoryReclaimer.get();
    _indexerParam.readerOpenType = DiskIndexerParameter::READER_NORMAL;

    MemIndexerParameter memIndexerParam;
    auto memIndexer = std::make_unique<MultiValueAttributeMemIndexer<T>>(memIndexerParam);
    ASSERT_TRUE(memIndexer != nullptr);
    ASSERT_TRUE(memIndexer->Init(attrConfig, _docInfoExtractorFactory.get()).IsOK());

    autil::mem_pool::Pool pool;
    std::string lastDocStr;
    for (uint32_t i = 0; i < DOC_NUM; ++i) {
        // make duplicate attribute doc
        if (i % 2 == 1) {
            expectData.push_back(*expectData.rbegin());
            StringView encodeStr = memIndexer->_attrConvertor->Encode(StringView(lastDocStr), &pool);
            memIndexer->AddField((docid_t)i, encodeStr, false);
            continue;
        }
        uint32_t valueLen = i * 3 % 10;
        std::stringstream ss;
        std::vector<T> dataOneDoc;
        for (uint32_t j = 0; j < valueLen; j++) {
            if (j != 0) {
                ss << MULTI_VALUE_SEPARATOR;
            }

            int value = (i + j) * 3 % 128;
            ss << value;
            dataOneDoc.push_back((T)value);
        }
        expectData.push_back(dataOneDoc);
        lastDocStr = ss.str();
        StringView encodeStr = memIndexer->_attrConvertor->Encode(StringView(lastDocStr), &pool);
        memIndexer->AddField((docid_t)i, encodeStr, false);
    }

    // dump segmnet
    SimplePool dumpPool;
    ASSERT_TRUE(memIndexer->Dump(&dumpPool, _attrDir, nullptr).IsOK());
    if (!needCompress) {
        CheckOffsetFileLength(attrConfig, offsetUnitSize * (DOC_NUM + 1));
    }
}

void MultiValueAttributeDiskIndexerTest::CheckOffsetFileLength(std::shared_ptr<AttributeConfig>& attrConfig,
                                                               int64_t targetLength)
{
    std::string offsetFilePath =
        PathUtil::JoinPath(_attrDir->GetLogicalPath(), attrConfig->GetAttrName(), ATTRIBUTE_OFFSET_FILE_NAME);
    FileMeta fileMeta;
    fslib::ErrorCode err = FileSystem::getFileMeta(PathUtil::JoinPath(_rootPath, offsetFilePath), fileMeta);
    EXPECT_EQ(EC_OK, err);
    EXPECT_EQ(targetLength, fileMeta.fileLength);
}

template <typename T>
void MultiValueAttributeDiskIndexerTest::CheckRead(const std::vector<std::vector<T>>& expectedValues,
                                                   MultiValueAttributeDiskIndexer<T>& diskIndexer)
{
    bool isNull = false;
    for (size_t i = 0; i < expectedValues.size(); i++) {
        MultiValueType<T> value;
        auto ctx = diskIndexer.CreateReadContext(_pool.get());
        diskIndexer.Read(i, value, isNull, ctx);
        const std::vector<T>& expectedValue = expectedValues[i];
        ASSERT_EQ(expectedValue.size(), value.size());
        for (size_t j = 0; j < expectedValue.size(); ++j) {
            ASSERT_EQ(expectedValue[j], value[j]);
        }

        uint64_t dataLen = diskIndexer.TEST_GetDataLength((docid_t)i, _pool.get());
        uint8_t* buffer = (uint8_t*)_pool->allocate(dataLen);
        auto ctxBase = diskIndexer.CreateReadContextPtr(_pool.get());
        uint32_t readDataLen;
        ASSERT_TRUE(diskIndexer.Read((docid_t)i, ctxBase, buffer, dataLen, readDataLen, isNull));
    }
}

template <typename T>
void MultiValueAttributeDiskIndexerTest::MakeUpdateData(std::map<docid_t, std::vector<T>>& toUpdateDocs)
{
    for (uint32_t i = 0; i < UPDATE_DOC_NUM; i++) {
        docid_t docId = rand() % DOC_NUM;
        uint32_t valueCount = i;
        std::vector<T>& valueVector = toUpdateDocs[docId];
        for (uint32_t valueId = 0; valueId < valueCount; ++valueId) {
            T value = valueId + docId;
            valueVector.push_back(value);
        }
    }
}

template <typename T>
void MultiValueAttributeDiskIndexerTest::InnerTestRead(uint64_t offsetThreshold, uint64_t offsetUnitSize,
                                                       bool uniqEncode, bool needEqualCompress)
{
    return DoTestRead<T>(offsetThreshold, offsetUnitSize, uniqEncode, needEqualCompress, true);
    return DoTestRead<T>(offsetThreshold, offsetUnitSize, uniqEncode, needEqualCompress, false);
}

template <typename T>
void MultiValueAttributeDiskIndexerTest::DoTestRead(uint64_t offsetThreshold, uint64_t offsetUnitSize, bool uniqEncode,
                                                    bool needEqualCompress, bool defaultValue)
{
    std::string compressTypeStr =
        CompressTypeOption::GetCompressStr(uniqEncode, needEqualCompress, false, false, false, false, 0.0f);

    auto attrConfig = AttributeTestUtil::CreateAttrConfig<T>(
        /*isMultiValue*/ true, compressTypeStr);

    std::shared_ptr<FieldConfig> fieldConfig = attrConfig->GetFieldConfig();
    ASSERT_TRUE(attrConfig->SetCompressType(compressTypeStr).IsOK());
    attrConfig->SetU32OffsetThreshold(offsetThreshold);
    std::vector<std::vector<T>> expectData;
    if (defaultValue) {
        expectData = {{1, 2, 3, 4}, {1, 2, 3, 4}};
        std::stringstream ss;
        ss << "1" << MULTI_VALUE_SEPARATOR << "2" << MULTI_VALUE_SEPARATOR << "3" << MULTI_VALUE_SEPARATOR << "4";
        fieldConfig->SetDefaultValue(ss.str());
        _indexerParam.docCount = DOC_NUM;
        _indexerParam.readerOpenType = DiskIndexerParameter::READER_DEFAULT_VALUE;
    } else {
        MakeOneAttributeSegment<T>(expectData, attrConfig, offsetUnitSize, needEqualCompress);
    }
    auto metrics = std::make_shared<AttributeMetrics>(_metricsReporter);
    MultiValueAttributeDiskIndexer<T> diskIndexer(metrics, _indexerParam);
    ASSERT_TRUE(diskIndexer.Open(attrConfig, _attrDir->GetIDirectory()).IsOK());
    CheckRead<T>(expectData, diskIndexer);
}

template <typename T>
void MultiValueAttributeDiskIndexerTest::InnerTestUpdate(uint64_t offsetThreshold, uint64_t offsetUnitSize,
                                                         bool uniqEncode, bool needEqualCompress)
{
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<T>(/*isMultiValue*/ true, /*compressType*/ std::nullopt);
    std::string compressTypeStr = CompressTypeOption::GetCompressStr(
        uniqEncode, needEqualCompress, attrConfig->GetCompressType().HasPatchCompress(), false, false, false, 0.0f);

    auto fieldConfig = attrConfig->GetFieldConfig();
    ASSERT_TRUE(attrConfig->SetCompressType(compressTypeStr).IsOK());
    attrConfig->SetUpdatable(true);
    attrConfig->SetU32OffsetThreshold(offsetThreshold);

    std::vector<std::vector<T>> expectData;
    MakeOneAttributeSegment<T>(expectData, attrConfig, offsetUnitSize, needEqualCompress);

    auto metrics = std::make_shared<AttributeMetrics>(_metricsReporter);
    MultiValueAttributeDiskIndexer<T> diskIndexer(metrics, _indexerParam);
    ASSERT_TRUE(diskIndexer.Open(attrConfig, _attrDir->GetIDirectory()).IsOK());

    std::map<docid_t, std::vector<T>> toUpdateDocs;
    MakeUpdateData(toUpdateDocs);

    // update field
    bool isNull = false;
    typename std::map<docid_t, std::vector<T>>::iterator it = toUpdateDocs.begin();
    for (; it != toUpdateDocs.end(); it++) {
        docid_t docId = it->first;
        std::vector<T> value = it->second;

        size_t buffLen = MultiValueAttributeFormatter::GetEncodedCountLength(value.size()) + sizeof(T) * value.size();
        uint8_t buff[buffLen];

        size_t encodeLen = MultiValueAttributeFormatter::EncodeCount(value.size(), (char*)buff, buffLen);
        memcpy(buff + encodeLen, value.data(), sizeof(T) * value.size());
        diskIndexer.TEST_UpdateField(docId, autil::StringView((char*)buff, buffLen), isNull);
        expectData[docId] = value;
    }
    // repeatedly update same value test
    for (size_t i = 0; i < expectData.size(); i++) {
        MultiValueType<T> value;
        auto ctx = diskIndexer.CreateReadContext(nullptr);
        diskIndexer.Read(i, value, isNull, ctx);
        const std::vector<T>& expectedValue = expectData[i];
        ASSERT_EQ(expectedValue.size(), value.size());
        for (size_t j = 0; j < expectedValue.size(); ++j) {
            ASSERT_EQ(expectedValue[j], value[j]);
        }

        uint64_t dataLen = diskIndexer.TEST_GetDataLength((docid_t)i, _pool.get());
        uint8_t* buffer = (uint8_t*)_pool->allocate(dataLen);
        auto ctxBase = diskIndexer.CreateReadContextPtr(_pool.get());
        uint32_t readDataLen;
        ASSERT_TRUE(diskIndexer.Read((docid_t)i, ctxBase, buffer, dataLen, readDataLen, isNull));

        // repeatedly update same value test
        auto [status, offset] = diskIndexer.GetOffset((docid_t)i, ctxBase);
        ASSERT_TRUE(status.IsOK());
        for (size_t k = 0; k < 10; k++) {
            ASSERT_TRUE(diskIndexer.TEST_UpdateField((docid_t)i, autil::StringView((char*)buffer, dataLen), isNull));
            auto [status, curOffset] = diskIndexer.GetOffset((docid_t)i, ctxBase);
            ASSERT_TRUE(status.IsOK());
            ASSERT_EQ(offset, curOffset);
        }
    }
}

template <typename T>
void MultiValueAttributeDiskIndexerTest::InnerTestMemUsed(uint64_t offsetThreshold, uint64_t offsetUnitSize,
                                                          bool uniqEncode, bool needEqualCompress)
{
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<T>(/*isMultiValue*/ true, /*compressType*/ std::nullopt);
    std::string compressTypeStr = CompressTypeOption::GetCompressStr(
        uniqEncode, needEqualCompress, attrConfig->GetCompressType().HasPatchCompress(), false, false, false, 0.0f);

    std::shared_ptr<FieldConfig> fieldConfig = attrConfig->GetFieldConfig();
    ASSERT_TRUE(attrConfig->SetCompressType(compressTypeStr).IsOK());
    attrConfig->SetU32OffsetThreshold(offsetThreshold);

    std::vector<std::vector<T>> expectData;
    MakeOneAttributeSegment<T>(expectData, attrConfig, offsetUnitSize, needEqualCompress);

    auto metrics = std::make_shared<AttributeMetrics>(_metricsReporter);
    MultiValueAttributeDiskIndexer<T> diskIndexer(metrics, _indexerParam);
    auto estimateMemUsed = diskIndexer.EstimateMemUsed(attrConfig, _attrDir->GetIDirectory());
    ASSERT_TRUE(diskIndexer.Open(attrConfig, _attrDir->GetIDirectory()).IsOK());
    auto evaluateMemUsed = diskIndexer.EvaluateCurrentMemUsed();

    ASSERT_TRUE(estimateMemUsed > 0);
    ASSERT_EQ(estimateMemUsed, evaluateMemUsed);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForWriteRead)
{
    InnerTestRead<uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestRead<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestRead<float>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestRead<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestRead<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForWriteReadWithSmallOffsetThreshold)
{
    uint64_t smallOffsetThreshold = 16;
    InnerTestRead<uint32_t>(smallOffsetThreshold, sizeof(uint64_t), false, false);
    InnerTestRead<uint64_t>(smallOffsetThreshold, sizeof(uint64_t), false, false);
    InnerTestRead<float>(smallOffsetThreshold, sizeof(uint64_t), false, false);
    InnerTestRead<int8_t>(smallOffsetThreshold, sizeof(uint64_t), false, false);
    InnerTestRead<uint8_t>(smallOffsetThreshold, sizeof(uint64_t), false, false);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForWriteReadWithUniqEncode)
{
    InnerTestRead<uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, false);
    InnerTestRead<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, false);
    InnerTestRead<float>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, false);
    InnerTestRead<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, false);
    InnerTestRead<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, false);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForWriteReadWithUniqEncodeAndSmallThreshold)
{
    uint64_t smallOffsetThreshold = 16;
    InnerTestRead<uint32_t>(smallOffsetThreshold, sizeof(uint64_t), true, false);
    InnerTestRead<uint64_t>(smallOffsetThreshold, sizeof(uint64_t), true, false);
    InnerTestRead<float>(smallOffsetThreshold, sizeof(uint64_t), true, false);
    InnerTestRead<int8_t>(smallOffsetThreshold, sizeof(uint64_t), true, false);
    InnerTestRead<uint8_t>(smallOffsetThreshold, sizeof(uint64_t), true, false);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForWriteReadWithCompress)
{
    InnerTestRead<uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestRead<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestRead<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestRead<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForWriteReadWithSmallOffsetThresholdWithCompress)
{
    uint64_t smallOffsetThreshold = 16;
    InnerTestRead<uint32_t>(smallOffsetThreshold, sizeof(uint64_t), false, true);
    InnerTestRead<uint64_t>(smallOffsetThreshold, sizeof(uint64_t), false, true);
    InnerTestRead<int8_t>(smallOffsetThreshold, sizeof(uint64_t), false, true);
    InnerTestRead<uint8_t>(smallOffsetThreshold, sizeof(uint64_t), false, true);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForWriteReadWithUniqEncodeWithCompress)
{
    InnerTestRead<uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, true);
    InnerTestRead<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, true);
    InnerTestRead<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, true);
    InnerTestRead<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true, true);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForWriteReadWithUniqEncodeAndSmallThresholdWithCompress)
{
    uint64_t smallOffsetThreshold = 16;
    InnerTestRead<uint32_t>(smallOffsetThreshold, sizeof(uint64_t), true, true);
    InnerTestRead<uint64_t>(smallOffsetThreshold, sizeof(uint64_t), true, true);
    InnerTestRead<int8_t>(smallOffsetThreshold, sizeof(uint64_t), true, true);
    InnerTestRead<uint8_t>(smallOffsetThreshold, sizeof(uint64_t), true, true);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForUpdateField)
{
    InnerTestUpdate<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestUpdate<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestUpdate<int16_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestUpdate<uint16_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestUpdate<int32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestUpdate<int64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestUpdate<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);

    InnerTestUpdate<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestUpdate<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestUpdate<int16_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestUpdate<uint16_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestUpdate<int32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestUpdate<int64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestUpdate<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
}

TEST_F(MultiValueAttributeDiskIndexerTest, TestCaseForMemUsed)
{
    InnerTestMemUsed<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestMemUsed<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestMemUsed<int16_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestMemUsed<uint16_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestMemUsed<int32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestMemUsed<int64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);
    InnerTestMemUsed<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, true);

    InnerTestMemUsed<int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestMemUsed<uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestMemUsed<int16_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestMemUsed<uint16_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestMemUsed<int32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestMemUsed<int64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
    InnerTestMemUsed<uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), false, false);
}

} // namespace indexlibv2::index
