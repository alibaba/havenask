#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/new_pfordelta_int_encoder.h"

using namespace std;

IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(common);

class NewPForDeltaInt32EncoderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(NewPForDeltaInt32EncoderTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForDecode()
    {
        const static size_t DECODE_TIMES = 1000;
        for (size_t i = 0; i < DECODE_TIMES; ++i)
        {
            uint32_t buffer[MAX_RECORD_SIZE];
            uint32_t dataSize = ((i * 13) % MAX_RECORD_SIZE) + 1;
            for (uint32_t j = 0; j < dataSize; ++j)
            {
                buffer[j] = j * 7 + i * 3;
            }
            NewPForDeltaInt32Encoder encoder;
            ByteSliceWriter sliceWriter;
            encoder.Encode(sliceWriter, buffer, dataSize);
            
            ByteSliceReader sliceReader(sliceWriter.GetByteSliceList());
            
            uint32_t dest[MAX_RECORD_SIZE];
            uint32_t decodedLen = encoder.Decode(dest, MAX_RECORD_SIZE, sliceReader);
            INDEXLIB_TEST_EQUAL(dataSize, decodedLen);
            
            for (uint32_t k = 0; k < dataSize; ++k)
            {
                INDEXLIB_TEST_EQUAL(buffer[k], dest[k]);
            }
        }
    }


    void TestCaseForDecodeWithBuffer()
    {
        const static size_t DECODE_TIMES = 1000;
        for (size_t i = 0; i < DECODE_TIMES; ++i)
        {
            uint32_t buffer[MAX_RECORD_SIZE];
            uint32_t dataSize = ((i * 13) % MAX_RECORD_SIZE) + 1;
            for (uint32_t j = 0; j < dataSize; ++j)
            {
                buffer[j] = j * 7 + i * 3;
            }
            NewPForDeltaInt32Encoder encoder;
            ByteSliceWriter sliceWriter;
            encoder.Encode(sliceWriter, buffer, dataSize);
            
            ByteSliceReader sliceReader(sliceWriter.GetByteSliceList());
            
            uint32_t dest[MAX_RECORD_SIZE];
            uint32_t decodedLen = encoder.Decode(dest, MAX_RECORD_SIZE, sliceReader);
            INDEXLIB_TEST_EQUAL(dataSize, decodedLen);
            
            for (uint32_t k = 0; k < dataSize; ++k)
            {
                INDEXLIB_TEST_EQUAL(buffer[k], dest[k]);
            }
        }
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(NewPForDeltaInt32EncoderTest, TestCaseForDecode);

IE_NAMESPACE_END(common);
