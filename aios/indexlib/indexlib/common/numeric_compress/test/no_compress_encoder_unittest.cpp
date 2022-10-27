#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_define.h"
#include "indexlib/common/numeric_compress/no_compress_int_encoder.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(common);

class NoCompressEncoderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(NoCompressEncoderTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForInt32Decoder()
    {
        uint32_t buffer[MAX_RECORD_SIZE];
        for (uint32_t i = 0; i < MAX_RECORD_SIZE; i++)
        {
            buffer[i] = (int32_t)i;
        }
        NoCompressInt32Encoder encoder;
        ByteSliceWriter sliceWriter;
        uint32_t encodeLen = encoder.Encode(sliceWriter, buffer, MAX_RECORD_SIZE);
        INDEXLIB_TEST_EQUAL(encodeLen, 
                            sizeof(uint8_t) + MAX_RECORD_SIZE * sizeof(int32_t));
        ByteSliceReader sliceReader(sliceWriter.GetByteSliceList());
        
        uint32_t dest[MAX_RECORD_SIZE];
        uint32_t decodeLen = encoder.Decode(dest, MAX_RECORD_SIZE, sliceReader);
        INDEXLIB_TEST_EQUAL(decodeLen, (uint32_t)MAX_RECORD_SIZE);

        for (uint32_t i = 0; i < MAX_RECORD_SIZE; i++)
        {
            INDEXLIB_TEST_EQUAL(buffer[i], dest[i]);
        }
    }

    void TestCaseForInt16Decoder()
    {
        uint16_t buffer[MAX_RECORD_SIZE];
        for (uint32_t i = 0; i < MAX_RECORD_SIZE; i++)
        {
            buffer[i] = (uint16_t)i;
        }
        NoCompressInt16Encoder encoder;
        ByteSliceWriter sliceWriter;
        uint32_t encodeLen = encoder.Encode(sliceWriter, buffer, MAX_RECORD_SIZE);
        INDEXLIB_TEST_EQUAL(encodeLen, 
                            sizeof(uint8_t) + MAX_RECORD_SIZE * sizeof(uint16_t));
        ByteSliceReader sliceReader(sliceWriter.GetByteSliceList());
        
        uint16_t dest[MAX_RECORD_SIZE];
        uint32_t decodeLen = encoder.Decode(dest, MAX_RECORD_SIZE, sliceReader);
        INDEXLIB_TEST_EQUAL(decodeLen, (uint32_t)MAX_RECORD_SIZE);

        for (uint32_t i = 0; i < MAX_RECORD_SIZE; i++)
        {
            INDEXLIB_TEST_EQUAL(buffer[i], dest[i]);
        }
    }

    void TestCaseForInt8Decoder()
    {
        uint8_t buffer[MAX_RECORD_SIZE];
        for (uint32_t i = 0; i < MAX_RECORD_SIZE; i++)
        {
            buffer[i] = (uint8_t)i;
        }
        NoCompressInt8Encoder encoder;
        ByteSliceWriter sliceWriter;
        uint32_t encodeLen = encoder.Encode(sliceWriter, buffer, MAX_RECORD_SIZE);
        INDEXLIB_TEST_EQUAL(encodeLen, 
                            sizeof(uint8_t) + MAX_RECORD_SIZE * sizeof(uint8_t));
        ByteSliceReader sliceReader(sliceWriter.GetByteSliceList());
        
        uint8_t dest[MAX_RECORD_SIZE];
        uint32_t decodeLen = encoder.Decode(dest, MAX_RECORD_SIZE, sliceReader);
        INDEXLIB_TEST_EQUAL(decodeLen, (uint32_t)MAX_RECORD_SIZE);

        for (uint32_t i = 0; i < MAX_RECORD_SIZE; i++)
        {
            INDEXLIB_TEST_EQUAL(buffer[i], dest[i]);
        }
    }


private:
};

INDEXLIB_UNIT_TEST_CASE(NoCompressEncoderTest, TestCaseForInt32Decoder);
INDEXLIB_UNIT_TEST_CASE(NoCompressEncoderTest, TestCaseForInt16Decoder);
INDEXLIB_UNIT_TEST_CASE(NoCompressEncoderTest, TestCaseForInt8Decoder);

IE_NAMESPACE_END(common);
