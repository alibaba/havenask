#ifndef __INDEXLIB_VARNUMATTRIBUTEDEFRAGSLICEARRAYTEST_H
#define __INDEXLIB_VARNUMATTRIBUTEDEFRAGSLICEARRAYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_defrag_slice_array.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Fp16Encoder.h"

namespace indexlib { namespace index {

// TODO: enable null case
class VarNumAttributeDefragSliceArrayTest : public INDEXLIB_TESTBASE
{
private:
    typedef VarNumAttributeDefragSliceArray::SliceHeader SliceHeader;

public:
    VarNumAttributeDefragSliceArrayTest();
    ~VarNumAttributeDefragSliceArrayTest();
    typedef VarNumAttributeReader<int16_t> Int16MultiValueAttributeReader;
    DEFINE_SHARED_PTR(Int16MultiValueAttributeReader);

    DECLARE_CLASS_NAME(VarNumAttributeDefragSliceArrayTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDefrag();
    void TestMetrics();
    void TestMoveDataTriggerDefrag();
    void TestMoveDataTriggerDefragForEncodeFloat();
    void TestNeedDefrag();

private:
    void CheckDoc(Int16MultiValueAttributeReaderPtr reader, docid_t docid, uint8_t* expectValue);
    util::BytesAlignedSliceArrayPtr PrepareSliceArrayFile(const file_system::DirectoryPtr& partitionDirectory,
                                                          const std::string& attrDir);

    template <typename T>
    char* MakeBuffer(const std::string& valueStr)
    {
        std::vector<T> valueVec;
        autil::StringUtil::fromString(valueStr, valueVec, ",");

        size_t bufferLen =
            common::VarNumAttributeFormatter::GetEncodedCountLength(valueVec.size()) + valueVec.size() * sizeof(T);

        char* buffer = (char*)mPool->allocate(bufferLen);
        assert(buffer);

        size_t countLen = common::VarNumAttributeFormatter::EncodeCount(valueVec.size(), buffer, bufferLen);
        memcpy(buffer + countLen, valueVec.data(), valueVec.size() * sizeof(T));
        return buffer;
    }

    char* MakeEncodeFloatBuffer(const std::string& valueStr)
    {
        std::vector<float> valueVec;
        autil::StringUtil::fromString(valueStr, valueVec, ",");

        autil::StringView value = util::Fp16Encoder::Encode((const float*)valueVec.data(), valueVec.size(), mPool);
        return (char*)value.data();
    }

    template <typename T>
    size_t GetBufferLen(const char* buffer)
    {
        size_t encodeCountLen = 0;
        bool isNull = false;
        uint32_t count = common::VarNumAttributeFormatter::DecodeCount(buffer, encodeCountLen, isNull);
        return encodeCountLen + count * sizeof(T);
    }

private:
    autil::mem_pool::Pool* mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDefragSliceArrayTest, TestDefrag);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDefragSliceArrayTest, TestMetrics);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDefragSliceArrayTest, TestMoveDataTriggerDefrag);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDefragSliceArrayTest, TestMoveDataTriggerDefragForEncodeFloat);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeDefragSliceArrayTest, TestNeedDefrag);
}} // namespace indexlib::index

#endif //__INDEXLIB_VARNUMATTRIBUTEDEFRAGSLICEARRAYTEST_H
