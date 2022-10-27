#include "indexlib/index/normal/attribute/test/var_num_attribute_defrag_slice_array_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/util/slice_array/defrag_slice_array.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributeDefragSliceArrayTest);

VarNumAttributeDefragSliceArrayTest::VarNumAttributeDefragSliceArrayTest()
{
    mPool = new Pool();
}

VarNumAttributeDefragSliceArrayTest::~VarNumAttributeDefragSliceArrayTest()
{
    delete mPool;
}

void VarNumAttributeDefragSliceArrayTest::CaseSetUp()
{
    
}

void VarNumAttributeDefragSliceArrayTest::CaseTearDown()
{
}

BytesAlignedSliceArrayPtr VarNumAttributeDefragSliceArrayTest::PrepareSliceArrayFile(
        const DirectoryPtr& directory, const string& attrDir)
{
    DirectoryPtr attrDirectory = directory->GetDirectory(attrDir, true);
    SliceFilePtr sliceFile = attrDirectory->CreateSliceFile(
            ATTRIBUTE_DATA_EXTEND_SLICE_FILE_NAME, 32, 10);
    const BytesAlignedSliceArrayPtr& sliceArray = 
        sliceFile->CreateSliceFileReader()->GetBytesAlignedSliceArray();
    return sliceArray;
}

void VarNumAttributeDefragSliceArrayTest::TestMetrics()
{
    AttributeMetrics metrics;
    SingleFieldPartitionDataProvider provider;

    provider.Init(GET_TEST_DATA_PATH(), "int16:true:true", SFP_ATTRIBUTE);
    provider.Build("1,2,3,4", SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    
    //create sliceArray first to use smaller slice
    BytesAlignedSliceArrayPtr sliceArray = PrepareSliceArrayFile(
            partitionData->GetRootDirectory(), "segment_0_level_0/attribute/field/");
    Int16MultiValueAttributeReaderPtr reader(
            new Int16MultiValueAttributeReader(&metrics));
    reader->Open(provider.GetAttributeConfig(), partitionData);
    
    //update doc to fill first slice
    uint8_t* updateField = (uint8_t*)MakeBuffer<int16_t>("1,2,3");
    size_t bufferLen = GetBufferLen<int16_t>((const char*)updateField);

    reader->UpdateField(0, updateField, bufferLen);
    reader->UpdateField(1, updateField, bufferLen);
    reader->UpdateField(2, updateField, bufferLen);    
    reader->UpdateField(3, updateField, bufferLen);    

    ASSERT_EQ((int64_t)0, metrics.GetVarAttributeWastedBytesValue());

    //udpate doc trigger change slice
    uint8_t* reUpdateField = (uint8_t*)MakeBuffer<int16_t>("3,2,1");
    size_t reBufferLen = GetBufferLen<int16_t>((const char*)reUpdateField);

    reader->UpdateField(0, (uint8_t*)reUpdateField, reBufferLen);
    ASSERT_EQ((int64_t)reBufferLen, metrics.GetVarAttributeWastedBytesValue());
    ASSERT_EQ((int64_t)0, metrics.GetCurReaderReclaimableBytesValue());

    reader->UpdateField(1, (uint8_t*)reUpdateField, reBufferLen);
    ASSERT_EQ((int64_t)reBufferLen * 2, metrics.GetVarAttributeWastedBytesValue());
    ASSERT_EQ((int64_t)0, metrics.GetCurReaderReclaimableBytesValue());

    //trigger defrag
    reader->UpdateField(3, (uint8_t*)reUpdateField, reBufferLen);
    ASSERT_EQ((int64_t)28, metrics.GetVarAttributeWastedBytesValue());
    ASSERT_EQ((int64_t)0, metrics.GetReclaimedSliceCountValue());
    ASSERT_EQ((int64_t)32, metrics.GetCurReaderReclaimableBytesValue());

    reader.reset();
    ASSERT_EQ((int64_t)28, metrics.GetVarAttributeWastedBytesValue());
    ASSERT_EQ((int64_t)1, metrics.GetReclaimedSliceCountValue());
}

void VarNumAttributeDefragSliceArrayTest::TestDefrag()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "int16:true:true", SFP_ATTRIBUTE);
    provider.Build("1,2,3,4", SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();

    //create sliceArray first to use smaller slice
    BytesAlignedSliceArrayPtr sliceArray = PrepareSliceArrayFile(
            partitionData->GetRootDirectory(), "segment_0_level_0/attribute/field/");

    Int16MultiValueAttributeReaderPtr reader(new Int16MultiValueAttributeReader);
    reader->Open(provider.GetAttributeConfig(), partitionData);
    
    //update doc to fill first slice
    uint8_t* updateField = (uint8_t*)MakeBuffer<int16_t>("1,2,3");
    size_t bufferLen = GetBufferLen<int16_t>((const char*)updateField);

    reader->UpdateField(0, updateField, bufferLen);
    reader->UpdateField(1, updateField, bufferLen);
    reader->UpdateField(2, updateField, bufferLen);    
    reader->UpdateField(3, updateField, bufferLen);    

    SliceHeader* header = (SliceHeader*)sliceArray->GetValueAddress(0);
    ASSERT_EQ((int32_t)0, header->wastedSize);

    //udpate doc trigger change slice
    uint8_t* reUpdateField = (uint8_t*)MakeBuffer<int16_t>("3,2,1");
    size_t reBufferLen = GetBufferLen<int16_t>((const char*)reUpdateField);

    reader->UpdateField(0, (uint8_t*)reUpdateField, reBufferLen);
    ASSERT_EQ((int32_t)bufferLen, header->wastedSize);
    
    reader->UpdateField(1, (uint8_t*)reUpdateField, reBufferLen);
    ASSERT_EQ((int32_t)bufferLen * 2, header->wastedSize);

    //trigger defrag
    reader->UpdateField(3, (uint8_t*)reUpdateField, reBufferLen);
    ASSERT_EQ((int32_t)32, header->wastedSize);

    sliceArray->ReleaseSlice(0);
    CheckDoc(reader, 2, (uint8_t*)updateField, bufferLen);
    CheckDoc(reader, 0, (uint8_t*)reUpdateField, reBufferLen);
    CheckDoc(reader, 1, (uint8_t*)reUpdateField, reBufferLen);
    CheckDoc(reader, 3, (uint8_t*)reUpdateField, reBufferLen);
}

void VarNumAttributeDefragSliceArrayTest::CheckDoc(
        Int16MultiValueAttributeReaderPtr reader, docid_t docid,
        uint8_t* expectValue, size_t valueSize)
{
    uint8_t value[32];
    reader->Read(docid, (uint8_t*)value, 32);
    ASSERT_TRUE(memcmp(expectValue, value, valueSize) == 0);
}

void VarNumAttributeDefragSliceArrayTest::TestMoveDataTriggerDefrag()
{
    AttributeMetrics metrics;
    SingleFieldPartitionDataProvider provider;

    provider.Init(GET_TEST_DATA_PATH(), "int16:true:true", SFP_ATTRIBUTE);
    provider.Build("1,2,3,4", SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    
    //create sliceArray first to use smaller slice
    BytesAlignedSliceArrayPtr sliceArray = PrepareSliceArrayFile(
            partitionData->GetRootDirectory(), "segment_0_level_0/attribute/field/");
    Int16MultiValueAttributeReaderPtr reader(
            new Int16MultiValueAttributeReader(&metrics));
    reader->Open(provider.GetAttributeConfig(), partitionData);

    //update doc to fill first slice
    uint8_t* updateField3 = (uint8_t*)MakeBuffer<int16_t>("2");
    uint8_t* updateField5 = (uint8_t*)MakeBuffer<int16_t>("3,6");
    uint8_t* updateField7 = (uint8_t*)MakeBuffer<int16_t>("4,0,0");
    uint8_t* updateField15 = (uint8_t*)MakeBuffer<int16_t>("8,0,0,0,0,0,0");

    // slice 0: H:4 + 0:7 + 1:15 + T:6
    reader->UpdateField(0, (uint8_t*)updateField7, 7);
    reader->UpdateField(1, (uint8_t*)updateField15, 15);

    // slice 1: H:4 + 3:15 + 3:3 + 1:5 + T:5
    reader->UpdateField(3, (uint8_t*)updateField15, 15);
    reader->UpdateField(3, (uint8_t*)updateField3, 3);
    ASSERT_EQ((int64_t)0, metrics.GetReclaimedSliceCountValue());
    ASSERT_EQ((int64_t)(6+15), metrics.GetVarAttributeWastedBytesValue());
    // trigger defrag slice 0
    reader->UpdateField(1, (uint8_t*)updateField5, 5);
    ASSERT_EQ((int64_t)56, metrics.GetVarAttributeWastedBytesValue());

    CheckDoc(reader, 0, (uint8_t*)updateField7, 7);
    CheckDoc(reader, 1, (uint8_t*)updateField5, 5);
    CheckDoc(reader, 3, (uint8_t*)updateField3, 3);

    reader.reset();
    ASSERT_EQ((int64_t)2, metrics.GetReclaimedSliceCountValue());
    ASSERT_EQ((int64_t)56, metrics.GetVarAttributeWastedBytesValue());
}

void VarNumAttributeDefragSliceArrayTest::TestMoveDataTriggerDefragForEncodeFloat()
{
    AttributeMetrics metrics;
    SingleFieldPartitionDataProvider provider;

    provider.Init(GET_TEST_DATA_PATH(), "float:true:true:fp16:4", SFP_ATTRIBUTE);
    provider.Build("1,2,3,4", SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    
    //create sliceArray first to use smaller slice
    BytesAlignedSliceArrayPtr sliceArray = PrepareSliceArrayFile(
            partitionData->GetRootDirectory(), "segment_0_level_0/attribute/field/");
    FloatMultiValueAttributeReaderPtr reader(
            new FloatMultiValueAttributeReader(&metrics));
    reader->Open(provider.GetAttributeConfig(), partitionData);

    //update doc to fill first slice
    uint8_t* updateField8 = (uint8_t*)MakeEncodeFloatBuffer("2,3,4,5");
    uint8_t* newUpdateField8 = (uint8_t*)MakeEncodeFloatBuffer("3,4,5,6");    

    // slice 0: H:4 + 0:8 + 1:8 + 2:8 + T:4
    reader->UpdateField(0, (uint8_t*)updateField8, 8);
    reader->UpdateField(1, (uint8_t*)updateField8, 8);
    reader->UpdateField(2, (uint8_t*)updateField8, 8);    

    // slice 1: H:4 + 3:8 + 2:8 + 1:8 + T:4
    reader->UpdateField(3, (uint8_t*)updateField8, 8);
    reader->UpdateField(2, (uint8_t*)newUpdateField8, 8);
    reader->UpdateField(1, (uint8_t*)newUpdateField8, 8);
    // defrag slice1
    ASSERT_EQ((int64_t)32, metrics.GetVarAttributeWastedBytesValue());    
    reader->UpdateField(0, (uint8_t*)newUpdateField8, 8);    
    ASSERT_EQ((int64_t)40, metrics.GetVarAttributeWastedBytesValue());

    reader.reset();
    ASSERT_EQ((int64_t)1, metrics.GetReclaimedSliceCountValue());    
}

void VarNumAttributeDefragSliceArrayTest::TestNeedDefrag()
{
    SimpleMemoryQuotaControllerPtr controller(new SimpleMemoryQuotaController(
                    MemoryQuotaControllerCreator::CreateBlockMemoryController()));
    DefragSliceArray::SliceArrayPtr sliceArray(
            new DefragSliceArray::SliceArray(controller, 16, 3));
    VarNumAttributeDefragSliceArray defragArray(sliceArray, "test", 0.5);

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

IE_NAMESPACE_END(index);

