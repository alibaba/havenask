#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"

IE_NAMESPACE_BEGIN(common);

class VbyteCompressorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(VbyteCompressorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForEncodeAndDecodeInt32()
    {
        int32_t value = 1212;
        uint8_t buffer[4];
        uint32_t n = VByteCompressor::EncodeVInt32(buffer, 4, value);
        INDEXLIB_TEST_EQUAL((uint32_t)2, n);
        
        uint32_t len = 4;
        uint8_t* input = buffer;
        int32_t valueExpect = VByteCompressor::DecodeVInt32(input, len);
        INDEXLIB_TEST_EQUAL(1212, valueExpect);
        INDEXLIB_TEST_EQUAL((uint32_t)2, len);
        INDEXLIB_TEST_EQUAL((uint32_t)2, (uint32_t)(input - buffer));
    }

    void TestCaseForEncodeAndDecodeBigInt32()
    {
        int32_t value = 12120000;
        uint8_t buffer[4];
        uint32_t n = VByteCompressor::EncodeVInt32(buffer, 4, value);
        INDEXLIB_TEST_EQUAL((uint32_t)4, n);
        
        uint32_t len = 4;
        uint8_t* inputByte = buffer;
        int32_t valueExpect = VByteCompressor::DecodeVInt32(inputByte, len);
        INDEXLIB_TEST_EQUAL(12120000, valueExpect);
        INDEXLIB_TEST_EQUAL((uint32_t)0, len);
        INDEXLIB_TEST_EQUAL((uint32_t)4, (uint32_t)(inputByte - buffer));
    }

    void TestCaseForGetVInt32Length()
    {
        int32_t value = 127;
        uint32_t n = VByteCompressor::GetVInt32Length(value);
        INDEXLIB_TEST_EQUAL((uint32_t)1, n);

        value = 3232;
        n = VByteCompressor::GetVInt32Length(value);
        INDEXLIB_TEST_EQUAL((uint32_t)2, n);

        value = 32323232;
        n = VByteCompressor::GetVInt32Length(value);
        INDEXLIB_TEST_EQUAL((uint32_t)4, n);

        value = 0x7FFFFFFF;
        n = VByteCompressor::GetVInt32Length(value);
        INDEXLIB_TEST_EQUAL((uint32_t)5, n);
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(VbyteCompressorTest, TestCaseForEncodeAndDecodeInt32);
INDEXLIB_UNIT_TEST_CASE(VbyteCompressorTest, TestCaseForEncodeAndDecodeBigInt32);
INDEXLIB_UNIT_TEST_CASE(VbyteCompressorTest, TestCaseForGetVInt32Length);

IE_NAMESPACE_END(common);
