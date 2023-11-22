#include "indexlib/index/attribute/MultiValueAttributeMemIndexer.h"

#include <random>

#include "autil/MultiValueCreator.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "indexlib/index/common/AttributeValueTypeTraits.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlibv2::config;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class MultiValueAttributeMemIndexerTest : public TESTBASE
{
public:
    MultiValueAttributeMemIndexerTest();
    ~MultiValueAttributeMemIndexerTest();

public:
    void setUp() override;
    void tearDown() override;

private:
    template <typename T>
    std::unique_ptr<MultiValueAttributeMemIndexer<T>>
    PrepareMemIndexer(const std::shared_ptr<AttributeConfig>& attrConfig);

    template <typename T>
    void TestAddField(uint32_t docCount, uint32_t valueCountPerField, uint64_t offsetThreshold, int64_t offsetUnitSize);

    template <typename T>
    void InnerTestSortDump(std::optional<std::string> compressType);

    template <typename T>
    void InnerTestAddEncodeField(uint32_t uniqDocCount, uint32_t valueCountPerField, uint32_t loop,
                                 uint64_t offsetThreshold = indexlib::index::VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD,
                                 int64_t offsetUnitSize = sizeof(uint32_t));

    void InnerTestAddEncodeFloatField(uint32_t fixedValueCount, const std::string& compressTypeStr,
                                      uint64_t offsetThreshold, int64_t offsetUnitSize, uint32_t docCount,
                                      uint32_t loop, bool sortDump);

    template <typename T>
    void InnerTestDumpCompressedOffset(bool isUpdatable, bool sortDump, size_t expectLen);

    void PrepareData(std::vector<std::string>& fields, uint32_t uniqDocCount, uint32_t valueCountPerField,
                     uint32_t loop);

    void CheckDataInfo(const std::shared_ptr<Directory>& dir, const std::string& attrName, uint32_t uniqItemCount,
                       uint32_t maxItemLen);

protected:
    std::string _rootDir;
    autil::mem_pool::Pool _pool;
    bool _alreadRegister = false;
    std::unique_ptr<indexlib::util::BuildResourceMetrics> _buildResourceMetrics;
    indexlib::util::BuildResourceMetricsNode* _buildResourceMetricsNode;
    std::shared_ptr<index::BuildingIndexMemoryUseUpdater> _memoryUseUpdater;
    std::shared_ptr<document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
};

MultiValueAttributeMemIndexerTest::MultiValueAttributeMemIndexerTest() {}

MultiValueAttributeMemIndexerTest::~MultiValueAttributeMemIndexerTest() {}

void MultiValueAttributeMemIndexerTest::setUp()
{
    _rootDir = GET_TEMP_DATA_PATH();
    _docInfoExtractorFactory = std::make_shared<plain::DocumentInfoExtractorFactory>();

    if (!_alreadRegister) {
        _buildResourceMetrics = std::make_unique<indexlib::util::BuildResourceMetrics>();
        _buildResourceMetrics->Init();
        _alreadRegister = true;
    }
    _buildResourceMetricsNode = _buildResourceMetrics->AllocateNode();
    _memoryUseUpdater = std::make_shared<index::BuildingIndexMemoryUseUpdater>(_buildResourceMetricsNode);
}

void MultiValueAttributeMemIndexerTest::tearDown() {}

//////////////////////////////////////////////////////////////
template <typename T>
void MultiValueAttributeMemIndexerTest::TestAddField(uint32_t docCount, uint32_t valueCountPerField,
                                                     uint64_t offsetThreshold, int64_t offsetUnitSize)
{
    AttributeTestUtil::ResetDir(_rootDir);
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<T>(/*isMultiValue*/ true, /*compressType*/ std::nullopt);
    attrConfig->SetU32OffsetThreshold(offsetThreshold);

    auto memIndexer = PrepareMemIndexer<T>(attrConfig);

    std::vector<std::string> fields;
    PrepareData(fields, docCount, valueCountPerField, 0);
    size_t dataLen = 0;
    autil::mem_pool::Pool pool;
    int64_t factor = memIndexer->InitDumpEstimateFactor();
    auto fieldsCnt = fields.size();
    for (size_t i = 0; i < fieldsCnt; i++) {
        autil::StringView encodeStr = memIndexer->_attrConvertor->Encode(autil::StringView(fields[i]), &pool);
        memIndexer->AddField((docid_t)i, encodeStr, /*isNull*/ false);
        dataLen +=
            (valueCountPerField * sizeof(T) + MultiValueAttributeFormatter::GetEncodedCountLength(valueCountPerField));
    }
    memIndexer->UpdateMemUse(_memoryUseUpdater.get());
    EXPECT_EQ(dataLen + fieldsCnt * sizeof(uint64_t), _buildResourceMetricsNode->GetValue(BMT_DUMP_FILE_SIZE));
    EXPECT_EQ(fieldsCnt * factor, _buildResourceMetricsNode->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));

    std::shared_ptr<Directory> outputDir;
    AttributeTestUtil::DumpWriter(*memIndexer, _rootDir, nullptr, outputDir);

    // check data file
    std::string dataFilePath = _rootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
    std::string offsetFilePath = _rootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_OFFSET_FILE_NAME;
    fslib::FileMeta fileMeta;
    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(dataFilePath, fileMeta));
    EXPECT_EQ((int64_t)dataLen, fileMeta.fileLength);

    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(offsetFilePath, fileMeta));
    EXPECT_EQ(offsetUnitSize * (docCount + 1), fileMeta.fileLength);
}

template <typename T>
void MultiValueAttributeMemIndexerTest::InnerTestAddEncodeField(uint32_t uniqDocCount, uint32_t valueCountPerField,
                                                                uint32_t loop, uint64_t offsetThreshold,
                                                                int64_t offsetUnitSize)
{
    AttributeTestUtil::ResetDir(_rootDir);
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<T>(/*isMultiValue*/ true, /*compressType*/ std::nullopt);
    attrConfig->SetU32OffsetThreshold(offsetThreshold);
    ASSERT_TRUE(attrConfig->SetCompressType("uniq").IsOK());

    auto memIndexer = PrepareMemIndexer<T>(attrConfig);
    autil::mem_pool::Pool pool;
    std::vector<std::string> fields;
    PrepareData(fields, uniqDocCount, valueCountPerField, loop);
    EXPECT_EQ(uniqDocCount * (loop + 1), fields.size());

    for (size_t i = 0; i < fields.size(); i++) {
        autil::StringView encodeStr = memIndexer->_attrConvertor->Encode(autil::StringView(fields[i]), &pool);
        memIndexer->AddField((docid_t)i, encodeStr, /*isNull*/ false);
    }

    uint32_t unitSize =
        MultiValueAttributeFormatter::GetEncodedCountLength(valueCountPerField) + valueCountPerField * sizeof(T);
    std::shared_ptr<Directory> outputDir;
    AttributeTestUtil::DumpWriter(*memIndexer, _rootDir, nullptr, outputDir);

    // check data file
    std::string dataFilePath = _rootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
    std::string offsetFilePath = _rootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_OFFSET_FILE_NAME;
    fslib::FileMeta fileMeta;
    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(dataFilePath, fileMeta));
    EXPECT_EQ((int64_t)uniqDocCount * unitSize, fileMeta.fileLength);

    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(offsetFilePath, fileMeta));
    EXPECT_EQ((int64_t)(offsetUnitSize * (fields.size() + 1)), fileMeta.fileLength);

    auto reader(MemFileNodeCreator::TEST_Create());
    EXPECT_EQ(FSEC_OK, reader->Open("", offsetFilePath, FSOT_MEM, -1));
    EXPECT_EQ(FSEC_OK, reader->Populate());

    void* buffer = reader->GetBaseAddress();
    for (size_t i = 0; i < fields.size(); i++) {
        uint64_t value = 0;
        if (offsetUnitSize == sizeof(uint32_t)) {
            value = *((uint32_t*)buffer + i);
        } else {
            value = *((uint64_t*)buffer + i);
        }
        EXPECT_EQ(unitSize * (i % uniqDocCount), value);
    }
    ASSERT_EQ(FSEC_OK, reader->Close());
    delete reader;
}

template <typename T>
std::unique_ptr<MultiValueAttributeMemIndexer<T>>
MultiValueAttributeMemIndexerTest::PrepareMemIndexer(const std::shared_ptr<AttributeConfig>& attrConfig)
{
    MemIndexerParameter indexerParam;
    auto memIndexer = std::make_unique<MultiValueAttributeMemIndexer<T>>(indexerParam);
    if (memIndexer->Init(attrConfig, _docInfoExtractorFactory.get()).IsOK()) {
        return memIndexer;
    }
    // init failed
    return nullptr;
}

void MultiValueAttributeMemIndexerTest::PrepareData(vector<string>& fields, uint32_t uniqDocCount,
                                                    uint32_t valueCountPerField, uint32_t loop)
{
    vector<string> uniqFields;
    for (uint32_t i = 0; i < uniqDocCount; i++) {
        stringstream ss;
        for (uint32_t j = 0; j < valueCountPerField; j++) {
            if (j != 0) {
                ss << MULTI_VALUE_SEPARATOR;
            }
            ss << i + j;
        }
        uniqFields.push_back(ss.str());
    }
    fields.insert(fields.end(), uniqFields.begin(), uniqFields.end());
    for (uint32_t i = 0; i < loop; i++) {
        fields.insert(fields.end(), uniqFields.begin(), uniqFields.end());
    }
}

template <typename T>
void MultiValueAttributeMemIndexerTest::InnerTestDumpCompressedOffset(bool isUpdatable, bool sortDump, size_t expectLen)
{
    AttributeTestUtil::ResetDir(_rootDir);
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<T>(/*isMultiValue*/ true, /*isCompressType*/ "equal");
    attrConfig->SetUpdatable(isUpdatable);

    std::vector<std::string> fields;
    PrepareData(fields, 2, 10, 0); // create 2 field(each 10 value)

    auto memIndexer = PrepareMemIndexer<T>(attrConfig);
    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < fields.size(); i++) {
        autil::StringView encodeStr = memIndexer->_attrConvertor->Encode(autil::StringView(fields[i]), &pool);
        memIndexer->AddField((docid_t)i, encodeStr, /*isNull*/ false);
    }

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    auto fs = FileSystemCreator::Create("test", _rootDir, fsOptions).GetOrThrow();
    auto dir = Directory::Get(fs)->MakeDirectory("dump_path");
    SimplePool dumpPool;
    std::shared_ptr<DocMapDumpParams> dumpParams(new DocMapDumpParams());
    if (sortDump) {
        dumpParams->new2old.resize(fields.size());
        dumpParams->old2new.resize(fields.size());
        for (size_t i = 0; i < fields.size(); i++) {
            dumpParams->old2new[i] = (i + 1) % fields.size();
            dumpParams->new2old[(i + 1) % fields.size()] = i;
        }
        ASSERT_TRUE(memIndexer->Dump(&dumpPool, dir, dynamic_pointer_cast<framework::DumpParams>(dumpParams)).IsOK());
    } else {
        ASSERT_TRUE(memIndexer->Dump(&dumpPool, dir, nullptr).IsOK());
    }

    std::string offsetFilePath = attrConfig->GetAttrName() + "/offset";
    size_t offsetLen = dir->GetFileLength(offsetFilePath);
    EXPECT_EQ(expectLen, offsetLen);

    DiskIndexerParameter param;
    param.docCount = fields.size();
    auto diskIndexer = std::make_unique<MultiValueAttributeDiskIndexer<T>>(nullptr, param);
    ASSERT_TRUE(diskIndexer->Open(attrConfig, dir->GetIDirectory()).IsOK());

    auto context = diskIndexer->CreateReadContext(&_pool);
    string sep(1, MULTI_VALUE_SEPARATOR);
    for (size_t i = 0; i < fields.size(); i++) {
        autil::MultiValueType<T> value;
        bool isNull = false;
        diskIndexer->Read(sortDump ? dumpParams->old2new[i] : i, value, isNull, context);
        ASSERT_EQ(fields[i], AttributeValueTypeToString<MultiValueType<T>>::ToString(value));
    }
}

void MultiValueAttributeMemIndexerTest::CheckDataInfo(const std::shared_ptr<Directory>& dir, const string& attrName,
                                                      uint32_t uniqItemCount, uint32_t maxItemLen)
{
    auto attrDir = dir->GetDirectory(attrName, false);
    ASSERT_TRUE(attrDir != nullptr);

    ASSERT_TRUE(attrDir->IsExist(ATTRIBUTE_DATA_INFO_FILE_NAME));
    string fileContent;
    attrDir->Load(ATTRIBUTE_DATA_INFO_FILE_NAME, fileContent);

    AttributeDataInfo dataInfo(/*count*/ 0, /*len*/ 0);
    dataInfo.FromString(fileContent);
    ASSERT_EQ(uniqItemCount, dataInfo.uniqItemCount);
    ASSERT_EQ(maxItemLen, dataInfo.maxItemLen);
}

void MultiValueAttributeMemIndexerTest::InnerTestAddEncodeFloatField(uint32_t fixedValueCount,
                                                                     const string& compressTypeStr,
                                                                     uint64_t offsetThreshold, int64_t offsetUnitSize,
                                                                     uint32_t docCount, uint32_t loop, bool sortDump)
{
    AttributeTestUtil::ResetDir(_rootDir);
    std::shared_ptr<AttributeConfig> attrConfig =
        AttributeTestUtil::CreateAttrConfig(ft_float, true, "attr_name", 0, compressTypeStr, fixedValueCount);
    attrConfig->SetU32OffsetThreshold(offsetThreshold);

    auto memIndexer = PrepareMemIndexer<float>(attrConfig);
    std::vector<std::string> fields;
    PrepareData(fields, docCount, fixedValueCount, loop);

    size_t fieldSize = fixedValueCount * sizeof(float);
    if (attrConfig->GetCompressType().HasFp16EncodeCompress()) {
        fieldSize = Fp16Encoder::GetEncodeBytesLen(fixedValueCount);
    } else if (attrConfig->GetCompressType().HasBlockFpEncodeCompress()) {
        fieldSize = BlockFpEncoder::GetEncodeBytesLen(fixedValueCount);
    } else if (attrConfig->GetCompressType().HasInt8EncodeCompress()) {
        fieldSize = FloatInt8Encoder::GetEncodeBytesLen(fixedValueCount);
    }

    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < fields.size(); i++) {
        autil::StringView encodeStr = memIndexer->_attrConvertor->Encode(autil::StringView(fields[i]), &pool);
        memIndexer->AddField((docid_t)i, encodeStr, /*isNull*/ false);
    }

    auto inMemReader = memIndexer->CreateInMemReader();
    ASSERT_TRUE(inMemReader != nullptr);
    auto floatInMemReader = std::dynamic_pointer_cast<MultiValueAttributeMemReader<float>>(inMemReader);
    ASSERT_TRUE(floatInMemReader != nullptr);

    for (size_t i = 0; i < fields.size(); i++) {
        MultiValueType<float> value;
        bool isNull = false;
        ASSERT_TRUE(floatInMemReader->Read(i, value, isNull, &pool));
        ASSERT_EQ(fields[i], AttributeValueTypeToString<MultiValueType<float>>::ToString(value));
    }

    std::shared_ptr<DocMapDumpParams> dumpParams(new DocMapDumpParams());
    if (sortDump) {
        dumpParams->new2old.resize(fields.size());
        dumpParams->old2new.resize(fields.size());
        for (size_t i = 0; i < fields.size(); i++) {
            dumpParams->old2new[i] = (i + 1) % fields.size();
            dumpParams->new2old[(i + 1) % fields.size()] = i;
        }
    }
    std::shared_ptr<Directory> outputDir;
    AttributeTestUtil::DumpWriter(*memIndexer, _rootDir, sortDump ? dumpParams : nullptr, outputDir);

    int64_t dataLen = attrConfig->IsUniqEncode() ? (int64_t)fieldSize * docCount : (int64_t)fieldSize * fields.size();

    // check data file
    std::string dataFilePath = _rootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
    std::string offsetFilePath = _rootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_OFFSET_FILE_NAME;
    fslib::FileMeta fileMeta;
    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(dataFilePath, fileMeta));
    EXPECT_EQ(dataLen, fileMeta.fileLength);
    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(offsetFilePath, fileMeta));
    EXPECT_EQ(offsetUnitSize * (fields.size() + 1), fileMeta.fileLength);

    // check dump value
    DiskIndexerParameter param;
    param.docCount = fields.size();
    auto diskIndexer = std::make_unique<MultiValueAttributeDiskIndexer<float>>(nullptr, param);
    ASSERT_TRUE(diskIndexer->Open(attrConfig, outputDir->GetIDirectory()).IsOK());
    auto context = diskIndexer->CreateReadContext(&_pool);
    string sep(1, MULTI_VALUE_SEPARATOR);
    for (size_t i = 0; i < fields.size(); i++) {
        autil::MultiValueType<float> value;
        bool isNull = false;
        diskIndexer->Read(sortDump ? dumpParams->old2new[i] : i, value, isNull, context);
        ASSERT_EQ(fields[i], AttributeValueTypeToString<MultiValueType<float>>::ToString(value));
    }
}

template <typename T>
void MultiValueAttributeMemIndexerTest::InnerTestSortDump(std::optional<std::string> compressType)
{
    std::shared_ptr<AttributeConfig> attrConfig = AttributeTestUtil::CreateAttrConfig<T>(true, compressType);
    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    autil::mem_pool::Pool pool;
    MemIndexerParameter memIndexerParameter;
    MultiValueAttributeMemIndexer<T> writer(memIndexerParameter);
    ASSERT_TRUE(writer.Init(attrConfig, _docInfoExtractorFactory.get()).IsOK());

    docid_t docCount = 500000;
    auto params = std::make_shared<DocMapDumpParams>();

    for (docid_t i = 0; i < docCount; ++i) {
        T value = i * 3 % 100;
        string fieldValue = StringUtil::toString(value) + MULTI_VALUE_SEPARATOR + StringUtil::toString(value + 1);
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

    DiskIndexerParameter indexerParameter;
    indexerParameter.docCount = docCount;
    auto indexer = std::make_shared<MultiValueAttributeDiskIndexer<T>>(nullptr, indexerParameter);
    ASSERT_TRUE(indexer->Open(attrConfig, dir->GetIDirectory()).IsOK());
    auto readCtx = indexer->CreateReadContext(nullptr);
    for (docid_t i = 0; i < docCount; ++i) {
        docid_t oldDocId = params->new2old[i];
        T mutilValue[2] = {(T)oldDocId * 3 % 100, (T)oldDocId * 3 % 100 + 1};
        autil::MultiValueType<T> value = autil::MultiValueCreator::createMultiValueBuffer(mutilValue, 2, &_pool);
        autil::MultiValueType<T> result;
        bool isNull = false;
        ASSERT_TRUE(indexer->Read(i, result, isNull, readCtx));
        ASSERT_EQ(value, result);
    }
}
TEST_F(MultiValueAttributeMemIndexerTest, TestCaseForUpdateField)
{
    auto attrConfig =
        AttributeTestUtil::CreateAttrConfig<uint8_t>(/*isMultiValue*/ true, /*compressType*/ std::nullopt);
    {
        SCOPED_TRACE("TEST for uniq update");
        int64_t dataLen = 0;
        ASSERT_TRUE(attrConfig->SetCompressType("uniq").IsOK());
        auto memIndexer = PrepareMemIndexer<uint8_t>(attrConfig);
        StringView encodeStr = memIndexer->_attrConvertor->Encode(StringView("1"), &_pool);
        memIndexer->AddField((docid_t)0, encodeStr, /*isNull*/ false);
        dataLen += memIndexer->_attrConvertor->Decode(encodeStr).data.size();
        memIndexer->UpdateMemUse(_memoryUseUpdater.get());
        EXPECT_EQ(1 * sizeof(uint64_t) + dataLen, _buildResourceMetricsNode->GetValue(BMT_DUMP_FILE_SIZE));

        encodeStr = memIndexer->_attrConvertor->Encode(StringView("2"), &_pool);
        memIndexer->AddField((docid_t)1, encodeStr, /*isNull*/ false);
        auto decodeStr = memIndexer->_attrConvertor->Decode(encodeStr).data;
        dataLen += decodeStr.size();
        memIndexer->UpdateMemUse(_memoryUseUpdater.get());
        EXPECT_EQ(2 * sizeof(uint64_t) + dataLen, _buildResourceMetricsNode->GetValue(BMT_DUMP_FILE_SIZE));

        memIndexer->TEST_UpdateField((docid_t)0, decodeStr, /*isNull*/ false);
        memIndexer->UpdateMemUse(_memoryUseUpdater.get());
        // TODO(yanke) why not plus updateData's size here
        dataLen += decodeStr.size();
        EXPECT_EQ(2 * sizeof(uint64_t) + dataLen, _buildResourceMetricsNode->GetValue(BMT_DUMP_FILE_SIZE));

        // data format: 1bytes = count, the left is data
        uint8_t* data = NULL;
        uint32_t dataLength = 0;
        memIndexer->_accessor->ReadData(0, data, dataLength);
        ASSERT_EQ((uint64_t)2, dataLength);
        ASSERT_EQ(2, (char)data[1]);
        ASSERT_EQ((uint64_t)12, memIndexer->_accessor->GetOffset(0));

        encodeStr = memIndexer->_attrConvertor->Encode(StringView("3"), &_pool);
        memIndexer->TEST_UpdateField((docid_t)1, encodeStr, /*isNull*/ false);
        memIndexer->_accessor->ReadData(1, data, dataLength);
        ASSERT_EQ((uint64_t)10, dataLength);
        ASSERT_EQ(3, (char)data[1]);
        ASSERT_EQ((uint64_t)18, memIndexer->_accessor->GetOffset(1));
    }
    {
        SCOPED_TRACE("TEST for normal update");
        int64_t dataLen = 0;
        // attrConfig->SetUniqEncode(/*isUniq*/ false);
        auto memIndexer = PrepareMemIndexer<uint8_t>(attrConfig);
        StringView encodeStr = memIndexer->_attrConvertor->Encode(StringView("1"), &_pool);
        memIndexer->AddField((docid_t)0, encodeStr, /*isNull*/ false);
        dataLen += memIndexer->_attrConvertor->Decode(encodeStr).data.size();
        memIndexer->UpdateMemUse(_memoryUseUpdater.get());
        EXPECT_EQ(1 * sizeof(uint64_t) + dataLen, _buildResourceMetricsNode->GetValue(BMT_DUMP_FILE_SIZE));
        int64_t dumpTempBufferSizeBefore = _buildResourceMetricsNode->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE);
        encodeStr = memIndexer->_attrConvertor->Encode(StringView("2"), &_pool);
        auto decodeStr = memIndexer->_attrConvertor->Decode(encodeStr).data;
        dataLen += decodeStr.size();
        memIndexer->TEST_UpdateField((docid_t)0, decodeStr, /*isNull*/ false);

        memIndexer->UpdateMemUse(_memoryUseUpdater.get());
        EXPECT_EQ(dumpTempBufferSizeBefore, _buildResourceMetricsNode->GetValue(BMT_DUMP_TEMP_MEMORY_SIZE));
        EXPECT_EQ(1 * sizeof(uint64_t) + dataLen, _buildResourceMetricsNode->GetValue(BMT_DUMP_FILE_SIZE));

        uint8_t* data = NULL;
        uint32_t dataLength = 0;
        memIndexer->_accessor->ReadData(0, data, dataLength);
        ASSERT_EQ((uint64_t)2, dataLength);
        ASSERT_EQ(2, (char)data[1]);
        ASSERT_EQ((uint64_t)6, memIndexer->_accessor->GetOffset(0));
    }
}

TEST_F(MultiValueAttributeMemIndexerTest, TestCaseForWrite)
{
    uint32_t docCount = 100;
    uint32_t valueCountPerField = 3;
    // default attribute data file threshold
    TestAddField<uint8_t>(docCount, valueCountPerField, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));
    TestAddField<uint32_t>(docCount, valueCountPerField, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));

    // small attribute data file threshold: easy to go beyond the threshold
    uint64_t smallOffsetThreshold = 16;
    TestAddField<uint8_t>(docCount, valueCountPerField, smallOffsetThreshold, sizeof(uint64_t));
    TestAddField<uint32_t>(docCount, valueCountPerField, smallOffsetThreshold, sizeof(uint64_t));

    // Boundary value for uint8
    uint64_t offsetThresholdForUint8 =
        docCount * (valueCountPerField * sizeof(uint8_t) +
                    MultiValueAttributeFormatter::GetEncodedCountLength(valueCountPerField));
    TestAddField<uint8_t>(docCount, valueCountPerField, offsetThresholdForUint8, sizeof(uint32_t));
    TestAddField<uint8_t>(docCount, valueCountPerField, offsetThresholdForUint8 - 1, sizeof(uint64_t));

    // Boundary value for uint32
    uint64_t offsetThresholdForUint32 =
        docCount * (valueCountPerField * sizeof(uint32_t) +
                    MultiValueAttributeFormatter::GetEncodedCountLength(valueCountPerField));
    TestAddField<uint32_t>(docCount, valueCountPerField, offsetThresholdForUint32, sizeof(uint32_t));
    TestAddField<uint32_t>(docCount, valueCountPerField, offsetThresholdForUint32 - 1, sizeof(uint64_t));
}

TEST_F(MultiValueAttributeMemIndexerTest, TestCaseForAddEncodeField)
{
    uint32_t docCount = 3;
    uint32_t valueCountPerField = 3;
    {
        SCOPED_TRACE("encode uint32_t");
        InnerTestAddEncodeField<uint32_t>(docCount, valueCountPerField, 2);
    }
    InnerTestAddEncodeField<int32_t>(docCount, valueCountPerField, 2);

    InnerTestAddEncodeField<uint8_t>(docCount, valueCountPerField, 4);
    InnerTestAddEncodeField<int8_t>(docCount, valueCountPerField, 3);

    uint64_t smallOffsetThreshold = 10;
    InnerTestAddEncodeField<uint32_t>(docCount, valueCountPerField, 3, smallOffsetThreshold, sizeof(uint64_t));
}

TEST_F(MultiValueAttributeMemIndexerTest, TestCaseForDumpPerf)
{
    auto attrConfig =
        AttributeTestUtil::CreateAttrConfig<uint32_t>(/*isMultiValue*/ true, /*compressType*/ std::nullopt);
    auto memIndexer = PrepareMemIndexer<uint32_t>(attrConfig);
    uint32_t valueCountPerField = 10;

    std::vector<std::string> fields;
    PrepareData(fields, 50, valueCountPerField, 0);
    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < fields.size(); i++) {
        autil::StringView encodeStr = memIndexer->_attrConvertor->Encode(autil::StringView(fields[i]), &pool);
        memIndexer->AddField((docid_t)i, encodeStr, /*isNull*/ false);
    }

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    auto fs = FileSystemCreator::Create("test", _rootDir, fsOptions).GetOrThrow();
    auto dir = Directory::Get(fs)->MakeDirectory("dump_path");

    int64_t beginTime = TimeUtility::currentTime();
    SimplePool dumpPool;
    ASSERT_TRUE(memIndexer->Dump(&dumpPool, dir, nullptr).IsOK());
    int64_t interval = TimeUtility::currentTime() - beginTime;

    cout << "***** time interval : " << interval / 1000 << "ms, " << endl;
}

TEST_F(MultiValueAttributeMemIndexerTest, TestCaseForDumpPerfForUniqField)
{
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<uint32_t>(/*isMultiValue*/ true, /*compressType*/ "equal");
    ASSERT_TRUE(attrConfig->SetCompressType("uniq").IsOK());

    auto memIndexer = PrepareMemIndexer<uint32_t>(attrConfig);
    uint32_t valueCountPerField = 10;

    std::vector<std::string> fields;
    PrepareData(fields, 50000, valueCountPerField, 10);
    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < fields.size(); i++) {
        autil::StringView encodeStr = memIndexer->_attrConvertor->Encode(autil::StringView(fields[i]), &pool);
        memIndexer->AddField((docid_t)i, encodeStr, /*isNull*/ false);
    }

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    auto fs = FileSystemCreator::Create("test", _rootDir, fsOptions).GetOrThrow();
    auto dir = Directory::Get(fs)->MakeDirectory("dump_path");

    int64_t beginTime = TimeUtility::currentTime();
    SimplePool dumpPool;
    ASSERT_TRUE(memIndexer->Dump(&dumpPool, dir, nullptr).IsOK());
    int64_t interval = TimeUtility::currentTime() - beginTime;

    cout << "***** time interval : " << interval / 1000 << "ms, " << endl;
}

TEST_F(MultiValueAttributeMemIndexerTest, TestCaseForDumpCompressedOffset)
{
    // expectLen : header(4+4) + slotItemMeta (8) + slotItemData(4+3*1) + tail(4) = 27
    InnerTestDumpCompressedOffset<uint32_t>(false, false, 27);
    InnerTestDumpCompressedOffset<uint32_t>(false, true, 27);

    // expectLen : header(4+4) + slotItemMeta (8) + slotItemData(8+8+3*1) + tail(4) = 39
    InnerTestDumpCompressedOffset<uint32_t>(true, false, 39);
    InnerTestDumpCompressedOffset<uint32_t>(true, true, 39);
}

TEST_F(MultiValueAttributeMemIndexerTest, TestCaseForDumpDataInfo)
{
    {
        SCOPED_TRACE("TEST for uniq update");
        auto attrConfig =
            AttributeTestUtil::CreateAttrConfig<uint8_t>(/*isMultiValue*/ true, /*compressType*/ std::nullopt);
        ASSERT_TRUE(attrConfig->SetCompressType("uniq").IsOK());
        auto memIndexer = PrepareMemIndexer<uint8_t>(attrConfig);
        StringView encodeStr = memIndexer->_attrConvertor->Encode(StringView("12"), &_pool);
        memIndexer->AddField((docid_t)0, encodeStr, /*isNull*/ false);

        encodeStr = memIndexer->_attrConvertor->Encode(StringView("234"), &_pool);
        memIndexer->AddField((docid_t)1, encodeStr, /*isNull*/ false);
        memIndexer->AddField((docid_t)2, encodeStr, /*isNull*/ false);
        memIndexer->AddField((docid_t)3, encodeStr, /*isNull*/ false);

        encodeStr = memIndexer->_attrConvertor->Encode(StringView("23456"), &_pool);
        auto decodeStr = memIndexer->_attrConvertor->Decode(encodeStr).data;
        memIndexer->TEST_UpdateField((docid_t)0, decodeStr, /*isNull*/ false);
        // data format: 1bytes = count, the left is data
        FileSystemOptions fsOptions;
        fsOptions.outputStorage = FSST_MEM;
        IFileSystemPtr fs = FileSystemCreator::Create("test", _rootDir, fsOptions).GetOrThrow();
        DirectoryPtr dir = Directory::Get(fs)->MakeDirectory("uniq_dump_path");
        SimplePool dumpPool;
        ASSERT_TRUE(memIndexer->Dump(&dumpPool, dir, nullptr).IsOK());
        CheckDataInfo(dir, "attr_name", 2, 6);

        // for sort
        std::shared_ptr<framework::DumpParams> params(new DocMapDumpParams());
        dynamic_pointer_cast<DocMapDumpParams>(params)->new2old = {3, 2, 1, 0};
        ASSERT_TRUE(memIndexer->Dump(&dumpPool, dir, params).IsOK());
        CheckDataInfo(dir, "attr_name", 2, 6);
    }

    {
        SCOPED_TRACE("TEST for normal update");
        auto attrConfig =
            AttributeTestUtil::CreateAttrConfig<uint8_t>(/*isMultiValue*/ true, /*compressType*/ std::nullopt);
        auto memIndexer = PrepareMemIndexer<uint8_t>(attrConfig);
        StringView encodeStr = memIndexer->_attrConvertor->Encode(StringView("12"), &_pool);
        memIndexer->AddField((docid_t)0, encodeStr, /*isNull*/ false);

        encodeStr = memIndexer->_attrConvertor->Encode(StringView("234"), &_pool);
        memIndexer->AddField((docid_t)1, encodeStr, /*isNull*/ false);
        memIndexer->AddField((docid_t)2, encodeStr, /*isNull*/ false);
        memIndexer->AddField((docid_t)3, encodeStr, /*isNull*/ false);

        encodeStr = memIndexer->_attrConvertor->Encode(StringView("23456"), &_pool);
        auto decodeStr = memIndexer->_attrConvertor->Decode(encodeStr).data;
        memIndexer->TEST_UpdateField((docid_t)0, decodeStr, /*isNull*/ false);
        // data format: 1bytes = count, the left is data
        FileSystemOptions fsOptions;
        fsOptions.outputStorage = FSST_MEM;
        IFileSystemPtr fs = FileSystemCreator::Create("test", _rootDir, fsOptions).GetOrThrow();
        DirectoryPtr dir = Directory::Get(fs)->MakeDirectory("normal_dump_path");
        SimplePool dumpPool;
        ASSERT_TRUE(memIndexer->Dump(&dumpPool, dir, nullptr).IsOK());
        CheckDataInfo(dir, "attr_name", 4, 6);

        // for sort
        std::shared_ptr<framework::DumpParams> params(new DocMapDumpParams());
        dynamic_pointer_cast<DocMapDumpParams>(params)->new2old = {3, 2, 1, 0};
        ASSERT_TRUE(memIndexer->Dump(&dumpPool, dir, params).IsOK());
        CheckDataInfo(dir, "attr_name", 4, 6);
    }
}

TEST_F(MultiValueAttributeMemIndexerTest, TestCaseForFloatEncode)
{
    InnerTestAddEncodeFloatField(4, "fp16", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), 100, 2, true);
    InnerTestAddEncodeFloatField(4, "fp16", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), 100, 2, false);
    InnerTestAddEncodeFloatField(4, "fp16|uniq", 16, sizeof(uint64_t), 100, 2, true);
    InnerTestAddEncodeFloatField(4, "fp16|uniq", 16, sizeof(uint64_t), 100, 2, false);

    InnerTestAddEncodeFloatField(4, "block_fp", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), 100, 2, true);
    InnerTestAddEncodeFloatField(4, "block_fp", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), 100, 2, false);
    InnerTestAddEncodeFloatField(4, "block_fp|uniq", 16, sizeof(uint64_t), 100, 2, true);
    InnerTestAddEncodeFloatField(4, "block_fp|uniq", 16, sizeof(uint64_t), 100, 2, false);

    InnerTestAddEncodeFloatField(4, "int8#127|uniq", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), 100, 2,
                                 true);
    InnerTestAddEncodeFloatField(4, "int8#127|uniq", VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), 100, 2,
                                 false);
    InnerTestAddEncodeFloatField(4, "int8#127", 16, sizeof(uint64_t), 100, 2, true);
    InnerTestAddEncodeFloatField(4, "int8#127", 16, sizeof(uint64_t), 100, 2, false);

    InnerTestAddEncodeFloatField(4, "", 16, sizeof(uint64_t), 100, 2, true);
    InnerTestAddEncodeFloatField(4, "", 16, sizeof(uint64_t), 100, 2, false);
}

TEST_F(MultiValueAttributeMemIndexerTest, TestSortDump)
{
    InnerTestSortDump<int32_t>(std::nullopt);
    InnerTestSortDump<int32_t>("equal");
    InnerTestSortDump<int32_t>("uniq");
    InnerTestSortDump<int32_t>("uniq|equal");
}

TEST_F(MultiValueAttributeMemIndexerTest, TestCompress)
{
    auto attrConfig =
        AttributeTestUtil::CreateAttrConfig<uint8_t>(/*isMultiValue*/ true, /*compressType*/ std::nullopt);
    std::shared_ptr<std::vector<SingleFileCompressConfig>> compressConfigs(new std::vector<SingleFileCompressConfig>);
    SingleFileCompressConfig simpleCompressConfig("simple", "zstd");

    compressConfigs->push_back(simpleCompressConfig);
    std::shared_ptr<FileCompressConfigV2> fileCompressConfig(new FileCompressConfigV2(compressConfigs, "simple"));
    attrConfig->SetFileCompressConfigV2(fileCompressConfig);

    ASSERT_TRUE(attrConfig->SetCompressType("uniq").IsOK());
    auto memIndexer = PrepareMemIndexer<uint8_t>(attrConfig);
    StringView encodeStr = memIndexer->_attrConvertor->Encode(StringView("12"), &_pool);
    memIndexer->AddField((docid_t)0, encodeStr, /*isNull*/ false);

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_MEM;
    IFileSystemPtr fs = FileSystemCreator::Create("test", _rootDir, fsOptions).GetOrThrow();
    DirectoryPtr dir = Directory::Get(fs)->MakeDirectory("uniq_dump_path");
    SimplePool dumpPool;
    ASSERT_TRUE(memIndexer->Dump(&dumpPool, dir, nullptr).IsOK());
    auto fieldDir = dir->GetDirectory(attrConfig->GetIndexName(), true);
    ASSERT_TRUE(fieldDir->IsExist("data"));
    auto compressInfo = fieldDir->GetCompressFileInfo("data");
    ASSERT_TRUE(compressInfo);
}

} // namespace indexlibv2::index
