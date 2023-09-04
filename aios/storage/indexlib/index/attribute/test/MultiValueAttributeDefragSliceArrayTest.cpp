#include "indexlib/index/attribute/MultiValueAttributeDefragSliceArray.h"

#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/MultiValueAttributeMemIndexer.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "indexlib/index/attribute/test/IndexMemoryReclaimerTest.h"
#include "indexlib/util/Fp16Encoder.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/slice_array/DefragSliceArray.h"
#include "unittest/unittest.h"

using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlibv2::config;

namespace indexlibv2::index {

class MultiValueAttributeDefragSliceArrayTest : public TESTBASE
{
public:
    MultiValueAttributeDefragSliceArrayTest();
    ~MultiValueAttributeDefragSliceArrayTest();

    void setUp() override;

public:
    void CheckDoc(std::shared_ptr<MultiValueAttributeDiskIndexer<int16_t>>& diskIndexer, docid_t docid,
                  uint8_t* expectValue);
    std::shared_ptr<BytesAlignedSliceArray> PrepareSliceArrayFile(const std::shared_ptr<Directory>& partitionDirectory,
                                                                  const std::string& attrDir);

    template <typename T>
    char* MakeBuffer(const std::string& valueStr);

    char* MakeEncodeFloatBuffer(const std::string& valueStr);

    template <typename T>
    size_t GetBufferLen(const char* buffer);

    void PrepareData(std::vector<std::string>& fields, uint32_t uniqDocCount, uint32_t valueCountPerField,
                     uint32_t loop);
    template <typename T>
    void MakeOneAttributeSegment(std::vector<T>& expectData, std::shared_ptr<AttributeConfig>& attrConfig,
                                 uint32_t uniqDocCount, uint32_t valueCountPerField);

private:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<Directory> _attrDir;
    std::shared_ptr<document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;
    IndexerParameter _indexerParam;
    std::string _rootPath;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::shared_ptr<framework::IIndexMemoryReclaimer> _indexMemoryReclaimer;
};

MultiValueAttributeDefragSliceArrayTest::MultiValueAttributeDefragSliceArrayTest()
{
    _pool = std::make_shared<autil::mem_pool::Pool>();
    _indexMemoryReclaimer = std::make_shared<IndexMemoryReclaimerTest>();
}

MultiValueAttributeDefragSliceArrayTest::~MultiValueAttributeDefragSliceArrayTest() {}

void MultiValueAttributeDefragSliceArrayTest::setUp()
{
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;
    _rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", _rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);
    _attrDir = rootDir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    assert(_attrDir != nullptr);
    _docInfoExtractorFactory = std::make_shared<plain::DocumentInfoExtractorFactory>();

    kmonitor::MetricsTags metricsTags;
    _metricsReporter = std::make_shared<kmonitor::MetricsReporter>("", metricsTags, "");
}

void MultiValueAttributeDefragSliceArrayTest::CheckDoc(
    std::shared_ptr<MultiValueAttributeDiskIndexer<int16_t>>& diskIndexer, docid_t docid, uint8_t* expectValue)
{
    autil::MultiValueType<int16_t> value;
    autil::MultiValueType<int16_t> expected;
    bool isNull = false;
    expected.init(expectValue);
    auto ctx = diskIndexer->CreateReadContext(nullptr);
    diskIndexer->Read(docid, value, isNull, ctx);
    ASSERT_EQ(expected, value);
}

std::shared_ptr<BytesAlignedSliceArray>
MultiValueAttributeDefragSliceArrayTest::PrepareSliceArrayFile(const std::shared_ptr<Directory>& directory,
                                                               const std::string& attrDir)
{
    DirectoryPtr attrDirectory = directory->GetDirectory(attrDir, true);
    FileWriterPtr sliceFileWriter =
        attrDirectory->CreateFileWriter(ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME, WriterOption::Slice(32, 10));
    sliceFileWriter->Close().GetOrThrow();
    SliceFileReaderPtr sliceFileReader = std::dynamic_pointer_cast<SliceFileReader>(
        attrDirectory->CreateFileReader(ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME, FSOT_SLICE));
    const BytesAlignedSliceArrayPtr& sliceArray = sliceFileReader->GetBytesAlignedSliceArray();
    return sliceArray;
}

template <typename T>
char* MultiValueAttributeDefragSliceArrayTest::MakeBuffer(const std::string& valueStr)
{
    std::vector<T> valueVec;
    autil::StringUtil::fromString(valueStr, valueVec, ",");

    size_t bufferLen =
        MultiValueAttributeFormatter::GetEncodedCountLength(valueVec.size()) + valueVec.size() * sizeof(T);

    char* buffer = (char*)_pool->allocate(bufferLen);
    assert(buffer);

    size_t countLen = MultiValueAttributeFormatter::EncodeCount(valueVec.size(), buffer, bufferLen);
    memcpy(buffer + countLen, valueVec.data(), valueVec.size() * sizeof(T));
    return buffer;
}

char* MultiValueAttributeDefragSliceArrayTest::MakeEncodeFloatBuffer(const std::string& valueStr)
{
    std::vector<float> valueVec;
    autil::StringUtil::fromString(valueStr, valueVec, ",");

    autil::StringView value = Fp16Encoder::Encode((const float*)valueVec.data(), valueVec.size(), _pool.get());
    return (char*)value.data();
}

template <typename T>
size_t MultiValueAttributeDefragSliceArrayTest::GetBufferLen(const char* buffer)
{
    size_t encodeCountLen = 0;
    bool isNull = false;
    uint32_t count = MultiValueAttributeFormatter::DecodeCount(buffer, encodeCountLen, isNull);
    return encodeCountLen + count * sizeof(T);
}

void MultiValueAttributeDefragSliceArrayTest::PrepareData(std::vector<std::string>& fields, uint32_t uniqDocCount,
                                                          uint32_t valueCountPerField, uint32_t loop)
{
    std::vector<std::string> uniqFields;
    for (uint32_t i = 0; i < uniqDocCount; i++) {
        std::stringstream ss;
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
void MultiValueAttributeDefragSliceArrayTest::MakeOneAttributeSegment(std::vector<T>& expectData,
                                                                      std::shared_ptr<AttributeConfig>& attrConfig,
                                                                      uint32_t uniqDocCount,
                                                                      uint32_t valueCountPerField)
{
    tearDown();
    setUp();

    std::vector<std::string> fields;
    PrepareData(fields, uniqDocCount, valueCountPerField, 0);
    _indexerParam.docCount = uniqDocCount;
    _indexerParam.indexMemoryReclaimer = _indexMemoryReclaimer.get();

    auto memIndexer = std::make_unique<MultiValueAttributeMemIndexer<T>>(_indexerParam);
    ASSERT_TRUE(memIndexer != nullptr);
    ASSERT_TRUE(memIndexer->Init(attrConfig, _docInfoExtractorFactory.get()).IsOK());

    autil::mem_pool::Pool pool;
    for (size_t i = 0; i < fields.size(); i++) {
        autil::StringView encodeStr = memIndexer->_attrConvertor->Encode(autil::StringView(fields[i]), &pool);
        memIndexer->AddField((docid_t)i, encodeStr, /*isNull*/ false);
    }
    // dump segmnet
    SimplePool dumpPool;
    ASSERT_TRUE(memIndexer->Dump(&dumpPool, _attrDir, nullptr).IsOK());
}

TEST_F(MultiValueAttributeDefragSliceArrayTest, TestDefrag)
{
    std::vector<int16_t> expectData;
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<int16_t>(/*isMultiValue*/ true, /*CompressType*/ "equal");
    attrConfig->SetUpdatable(true);
    MakeOneAttributeSegment<int16_t>(expectData, attrConfig, /*uniqDocCount*/ 4, /*valueCountPerField*/ 1);
    // create sliceArray first to use smaller slice
    BytesAlignedSliceArrayPtr sliceArray = PrepareSliceArrayFile(_attrDir, attrConfig->GetAttrName());

    std::shared_ptr<MultiValueAttributeDiskIndexer<int16_t>> diskIndexer =
        std::make_shared<MultiValueAttributeDiskIndexer<int16_t>>(nullptr, _indexerParam);
    ASSERT_TRUE(diskIndexer->Open(attrConfig, _attrDir->GetIDirectory()).IsOK());

    // update doc to fill first slice
    uint8_t* updateField = (uint8_t*)MakeBuffer<int16_t>("1,2,3");
    size_t bufferLen = GetBufferLen<int16_t>((const char*)updateField);

    diskIndexer->TEST_UpdateField(0, autil::StringView((char*)updateField, bufferLen), false);
    diskIndexer->TEST_UpdateField(1, autil::StringView((char*)updateField, bufferLen), false);
    diskIndexer->TEST_UpdateField(2, autil::StringView((char*)updateField, bufferLen), false);
    diskIndexer->TEST_UpdateField(3, autil::StringView((char*)updateField, bufferLen), false);

    MultiValueAttributeDefragSliceArray::SliceHeader* header =
        (MultiValueAttributeDefragSliceArray::SliceHeader*)sliceArray->GetValueAddress(0);
    ASSERT_EQ((int32_t)0, header->wastedSize);

    // udpate doc trigger change slice
    uint8_t* reUpdateField = (uint8_t*)MakeBuffer<int16_t>("3,2,1");
    size_t reBufferLen = GetBufferLen<int16_t>((const char*)reUpdateField);

    diskIndexer->TEST_UpdateField(0, autil::StringView((char*)reUpdateField, reBufferLen), false);
    ASSERT_EQ((int32_t)bufferLen, header->wastedSize);

    diskIndexer->TEST_UpdateField(1, autil::StringView((char*)reUpdateField, reBufferLen), false);
    ASSERT_EQ((int32_t)bufferLen * 2, header->wastedSize);

    // trigger defrag
    diskIndexer->TEST_UpdateField(3, autil::StringView((char*)reUpdateField, reBufferLen), false);
    CheckDoc(diskIndexer, 2, (uint8_t*)updateField);
    CheckDoc(diskIndexer, 0, (uint8_t*)reUpdateField);
    CheckDoc(diskIndexer, 1, (uint8_t*)reUpdateField);
    CheckDoc(diskIndexer, 3, (uint8_t*)reUpdateField);
}

TEST_F(MultiValueAttributeDefragSliceArrayTest, TestMetrics)
{
    auto metrics = std::make_shared<AttributeMetrics>(_metricsReporter);
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<int16_t>(/*isMultiValue*/ true, /*CompressType*/ "equal");
    attrConfig->SetUpdatable(true);
    std::vector<int16_t> expectData;
    MakeOneAttributeSegment<int16_t>(expectData, attrConfig, /*uniqDocCount*/ 4, /*valueCountPerField*/ 1);

    // create sliceArray first to use smaller slice
    BytesAlignedSliceArrayPtr sliceArray = PrepareSliceArrayFile(_attrDir, attrConfig->GetAttrName());
    std::shared_ptr<MultiValueAttributeDiskIndexer<int16_t>> diskIndexer =
        std::make_shared<MultiValueAttributeDiskIndexer<int16_t>>(metrics, _indexerParam);
    ASSERT_TRUE(diskIndexer->Open(attrConfig, _attrDir->GetIDirectory()).IsOK());

    // update doc to fill first slice
    uint8_t* updateField = (uint8_t*)MakeBuffer<int16_t>("1,2,3");
    size_t bufferLen = GetBufferLen<int16_t>((const char*)updateField);

    diskIndexer->TEST_UpdateField(0, autil::StringView((char*)updateField, bufferLen), false);
    diskIndexer->TEST_UpdateField(1, autil::StringView((char*)updateField, bufferLen), false);
    diskIndexer->TEST_UpdateField(2, autil::StringView((char*)updateField, bufferLen), false);
    diskIndexer->TEST_UpdateField(3, autil::StringView((char*)updateField, bufferLen), false);

    ASSERT_EQ((int64_t)0, metrics->GetvarAttributeWastedBytesValue());

    // udpate doc trigger change slice
    uint8_t* reUpdateField = (uint8_t*)MakeBuffer<int16_t>("3,2,1");
    size_t reBufferLen = GetBufferLen<int16_t>((const char*)reUpdateField);

    diskIndexer->TEST_UpdateField(0, autil::StringView((char*)reUpdateField, reBufferLen), false);
    ASSERT_EQ((int64_t)reBufferLen, metrics->GetvarAttributeWastedBytesValue());
    ASSERT_EQ((int64_t)0, metrics->GetcurReaderReclaimableBytesValue());

    diskIndexer->TEST_UpdateField(1, autil::StringView((char*)reUpdateField, reBufferLen), false);
    ASSERT_EQ((int64_t)reBufferLen * 2, metrics->GetvarAttributeWastedBytesValue());
    ASSERT_EQ((int64_t)0, metrics->GetcurReaderReclaimableBytesValue());

    // trigger defrag
    diskIndexer->TEST_UpdateField(3, autil::StringView((char*)reUpdateField, reBufferLen), false);
    ASSERT_EQ((int64_t)28, metrics->GetvarAttributeWastedBytesValue());
    ASSERT_EQ((int64_t)1, metrics->GetreclaimedSliceCountValue());
}

TEST_F(MultiValueAttributeDefragSliceArrayTest, TestMoveDataTriggerDefrag)
{
    auto metrics = std::make_shared<AttributeMetrics>(_metricsReporter);
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<int16_t>(/*isMultiValue*/ true, /*CompressType*/ "equal");
    attrConfig->SetUpdatable(true);
    std::vector<int16_t> expectData;
    MakeOneAttributeSegment<int16_t>(expectData, attrConfig, /*uniqDocCount*/ 4, /*valueCountPerField*/ 1);

    // create sliceArray first to use smaller slice
    BytesAlignedSliceArrayPtr sliceArray = PrepareSliceArrayFile(_attrDir, attrConfig->GetAttrName());
    std::shared_ptr<MultiValueAttributeDiskIndexer<int16_t>> diskIndexer =
        std::make_shared<MultiValueAttributeDiskIndexer<int16_t>>(metrics, _indexerParam);
    ASSERT_TRUE(diskIndexer->Open(attrConfig, _attrDir->GetIDirectory()).IsOK());

    // update doc to fill first slice
    uint8_t* updateField3 = (uint8_t*)MakeBuffer<int16_t>("2");
    uint8_t* updateField5 = (uint8_t*)MakeBuffer<int16_t>("3,6");
    uint8_t* updateField7 = (uint8_t*)MakeBuffer<int16_t>("4,0,0");
    uint8_t* updateField15 = (uint8_t*)MakeBuffer<int16_t>("8,0,0,0,0,0,0");

    // slice 0: H:4 + 0:7 + 1:15 + T:6
    diskIndexer->TEST_UpdateField(0, autil::StringView((char*)updateField7, 7), false);
    diskIndexer->TEST_UpdateField(1, autil::StringView((char*)updateField15, 15), false);

    // slice 1: H:4 + 3:15 + 3:3 + 1:5 + T:5
    diskIndexer->TEST_UpdateField(3, autil::StringView((char*)updateField15, 15), false);
    diskIndexer->TEST_UpdateField(3, autil::StringView((char*)updateField3, 3), false);
    ASSERT_EQ((int64_t)0, metrics->GetreclaimedSliceCountValue());
    ASSERT_EQ((int64_t)(6 + 15), metrics->GetvarAttributeWastedBytesValue());
    // trigger defrag slice 0
    diskIndexer->TEST_UpdateField(1, autil::StringView((char*)updateField5, 5), false);
    CheckDoc(diskIndexer, 0, (uint8_t*)updateField7);
    CheckDoc(diskIndexer, 1, (uint8_t*)updateField5);
    CheckDoc(diskIndexer, 3, (uint8_t*)updateField3);
    ASSERT_EQ((int64_t)2, metrics->GetreclaimedSliceCountValue());
    ASSERT_EQ((int64_t)56, metrics->GetvarAttributeWastedBytesValue());
}

TEST_F(MultiValueAttributeDefragSliceArrayTest, TestMoveDataTriggerDefragForEncodeFloat)
{
    auto metrics = std::make_shared<AttributeMetrics>(_metricsReporter);
    auto attrConfig = AttributeTestUtil::CreateAttrConfig(ft_float, true, "attr_name", 0, "fp16", 4);
    attrConfig->SetUpdatable(true);
    std::vector<float> expectData;
    MakeOneAttributeSegment<float>(expectData, attrConfig, /*uniqDocCount*/ 4, /*valueCountPerField*/ 1);

    // create sliceArray first to use smaller slice
    BytesAlignedSliceArrayPtr sliceArray = PrepareSliceArrayFile(_attrDir, attrConfig->GetAttrName());
    std::shared_ptr<MultiValueAttributeDiskIndexer<float>> diskIndexer =
        std::make_shared<MultiValueAttributeDiskIndexer<float>>(metrics, _indexerParam);
    ASSERT_TRUE(diskIndexer->Open(attrConfig, _attrDir->GetIDirectory()).IsOK());

    // update doc to fill first slice
    uint8_t* updateField8 = (uint8_t*)MakeEncodeFloatBuffer("2,3,4,5");
    uint8_t* newUpdateField8 = (uint8_t*)MakeEncodeFloatBuffer("3,4,5,6");

    // slice 0: H:4 + 0:8 + 1:8 + 2:8 + T:4
    diskIndexer->TEST_UpdateField(0, autil::StringView((char*)updateField8, 8), false);
    diskIndexer->TEST_UpdateField(1, autil::StringView((char*)updateField8, 8), false);
    diskIndexer->TEST_UpdateField(2, autil::StringView((char*)updateField8, 8), false);

    // slice 1: H:4 + 3:8 + 2:8 + 1:8 + T:4
    diskIndexer->TEST_UpdateField(3, autil::StringView((char*)updateField8, 8), false);
    diskIndexer->TEST_UpdateField(2, autil::StringView((char*)newUpdateField8, 8), false);
    diskIndexer->TEST_UpdateField(1, autil::StringView((char*)newUpdateField8, 8), false);
    // defrag slice1
    ASSERT_EQ((int64_t)32, metrics->GetvarAttributeWastedBytesValue());
    diskIndexer->TEST_UpdateField(0, autil::StringView((char*)newUpdateField8, 8), false);
    ASSERT_EQ((int64_t)40, metrics->GetvarAttributeWastedBytesValue());
    ASSERT_EQ((int64_t)1, metrics->GetreclaimedSliceCountValue());
}

TEST_F(MultiValueAttributeDefragSliceArrayTest, TestNeedDefrag)
{
    std::shared_ptr<SimpleMemoryQuotaController> controller =
        std::make_shared<SimpleMemoryQuotaController>(MemoryQuotaControllerCreator::CreateBlockMemoryController());
    std::shared_ptr<DefragSliceArray::SliceArray> sliceArray =
        std::make_shared<DefragSliceArray::SliceArray>(controller, 16, 3);
    MultiValueAttributeDefragSliceArray defragArray(sliceArray, "test", 50, _indexMemoryReclaimer.get());

    ASSERT_FALSE(defragArray.NeedDefrag(-1));
    ASSERT_EQ((uint64_t)4, defragArray.Append("", 0));
    ASSERT_FALSE(defragArray.NeedDefrag(0));

    ASSERT_EQ((uint64_t)4, defragArray.Append("123456789012", 12));

    int64_t sliceIdx = sliceArray->OffsetToSliceIdx(4);
    defragArray.IncreaseWastedSize(sliceIdx, 7);
    ASSERT_EQ(7, defragArray.GetWastedSize(0));
    ASSERT_EQ((uint64_t)20, defragArray.Append("0", 0));
    // 7/16<50%
    ASSERT_FALSE(defragArray.NeedDefrag(0));

    sliceIdx = sliceArray->OffsetToSliceIdx(11);
    defragArray.IncreaseWastedSize(sliceIdx, 1);
    ASSERT_EQ(8, defragArray.GetWastedSize(0));
    // 8/16=50%
    ASSERT_TRUE(defragArray.NeedDefrag(0));
}

} // namespace indexlibv2::index
