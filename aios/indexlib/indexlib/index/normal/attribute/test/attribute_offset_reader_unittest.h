#ifndef __INDEXLIB_ATTRIBUTEOFFSETREADERTEST_H
#define __INDEXLIB_ATTRIBUTEOFFSETREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/equivalent_compress_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_offset_reader.h"

IE_NAMESPACE_BEGIN(index);

class AttributeOffsetReaderTest : public INDEXLIB_TESTBASE {
public:
    AttributeOffsetReaderTest();
    ~AttributeOffsetReaderTest();
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestInit();
    void TestInitUncompressOffsetData();
    void TestGetOffset();
    void TestSetOffset();
    void TestGetOffsetPerfLongCaseTest();
    
private:
    void InnerTestOffsetReader(bool isUpdatable, const std::string& compressTypeStr,
                               uint32_t offsetThreshold, const std::string& offsetStr,
                               bool updateStep = 0 /* updateStep = 0, not update*/);

    void InnerTestGetOffsetPerf(uint32_t step);

    void PrepareUnCompressedOffsetBuffer(bool isU32Offset,
            uint32_t value[], uint32_t valueLen,
            uint8_t*& buffer, uint64_t& bufferLength);

    template <typename T>
    void PrepareCompressedOffsetBuffer(
            const std::vector<T>& offsets,
            uint8_t*& buffer, uint64_t& bufferLength)
    {
        common::EquivalentCompressWriter<T> writer;
        writer.Init(64);
        writer.CompressData(offsets);
        bufferLength = writer.GetCompressLength() + 4;
        buffer = new uint8_t[bufferLength];
        writer.DumpBuffer(buffer, bufferLength);
        uint32_t* magic = (uint32_t*)(buffer + bufferLength - 4);
        if (sizeof(T) == sizeof(uint64_t))
        {
            *magic = UINT64_OFFSET_TAIL_MAGIC;
        }
        else
        {
            assert(sizeof(T) == sizeof(uint32_t));
            *magic = UINT32_OFFSET_TAIL_MAGIC;
        }
    }

private:
    config::AttributeConfigPtr CreateAttrConfig(
            const std::string& compessTypeStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeOffsetReaderTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(AttributeOffsetReaderTest, TestGetOffset);
INDEXLIB_UNIT_TEST_CASE(AttributeOffsetReaderTest, TestSetOffset);
INDEXLIB_UNIT_TEST_CASE(AttributeOffsetReaderTest, TestInitUncompressOffsetData);
INDEXLIB_UNIT_TEST_CASE(AttributeOffsetReaderTest, TestGetOffsetPerfLongCaseTest);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTEOFFSETREADERTEST_H
