#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/reference_compress_int_encoder.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(common);

class ReferenceCompressInt32EncoderTest : public INDEXLIB_TESTBASE
{
public:

    DECLARE_CLASS_NAME(ReferenceCompressInt32EncoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForSimple();
    void TestCaseForUncompress();
private:
    void innerTestForEncodeDecode(const uint32_t base, const uint8_t lenByte, const size_t dataSize);

private:
    IE_LOG_DECLARE();
};


INDEXLIB_UNIT_TEST_CASE(ReferenceCompressInt32EncoderTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(ReferenceCompressInt32EncoderTest, TestCaseForUncompress);

IE_LOG_SETUP(common,  ReferenceCompressInt32EncoderTest);

void ReferenceCompressInt32EncoderTest::CaseSetUp()
{
}

void ReferenceCompressInt32EncoderTest::CaseTearDown()
{
}

void ReferenceCompressInt32EncoderTest::innerTestForEncodeDecode(
           const uint32_t base, const uint8_t lenByte, const size_t dataSize)
{
    vector<uint32_t> docIds;
    const uint32_t maxDelta = 1lu << (8 * lenByte - 1);
    size_t step = dataSize > 1 ? maxDelta / (dataSize - 1) : maxDelta;
    uint32_t cur = base;
    docIds.push_back(cur);
    for (size_t i = 1; i < dataSize; ++i)
    {
        cur += step;
        docIds.push_back(cur);
    }
    ReferenceCompressIntEncoder<uint32_t> encoder;
    ByteSliceWriter sliceWriter;
    auto writerSize = encoder.Encode(sliceWriter, docIds.data(), dataSize);
    INDEXLIB_TEST_EQUAL(sliceWriter.GetSize(), writerSize);
    uint32_t expectSize;
    if (dataSize == MAX_RECORD_SIZE && lenByte == sizeof(uint32_t))
    {
        expectSize = dataSize * sizeof(uint32_t);
    }
    else
    {
        expectSize = sizeof(int32_t) + sizeof(uint32_t) + (dataSize - 1) * lenByte;
    }
    INDEXLIB_TEST_EQUAL(expectSize, sliceWriter.GetSize());

    uint32_t destBuffer[MAX_RECORD_SIZE];
    uint32_t *dest = destBuffer;
    ReferenceCompressIntReader<uint32_t> reader;
    {
        ByteSliceReader sliceReader(sliceWriter.GetByteSliceList());
        INDEXLIB_TEST_EQUAL(dataSize, encoder.Decode(dest, MAX_RECORD_SIZE, sliceReader));
        INDEXLIB_TEST_EQUAL(base, encoder.GetFirstValue((char*)dest));
        reader.Reset((char*)dest);
        INDEXLIB_TEST_EQUAL(docIds[0], reader[0]);
        for (size_t i = 1; i < dataSize; ++i)
        {
            INDEXLIB_TEST_EQUAL(docIds[i], reader[i]);
            INDEXLIB_TEST_EQUAL(docIds[i], reader.Seek(docIds[i]));
        }
    }
    {
        ByteSliceReader sliceReader(sliceWriter.GetByteSliceList());
        INDEXLIB_TEST_EQUAL(dataSize, encoder.DecodeMayCopy(dest, MAX_RECORD_SIZE, sliceReader));
        INDEXLIB_TEST_EQUAL(base, encoder.GetFirstValue((char*)dest));
        reader.Reset((char*)dest);
        INDEXLIB_TEST_EQUAL(docIds[0], reader[0]);
        for (size_t i = 1; i < dataSize; ++i)
        {
            INDEXLIB_TEST_EQUAL(docIds[i], reader[i]);
            INDEXLIB_TEST_EQUAL(docIds[i], reader.Seek(docIds[i]));
        }
    }
}

void ReferenceCompressInt32EncoderTest::TestCaseForSimple()
{
    innerTestForEncodeDecode(4300, 1, 128);
    innerTestForEncodeDecode(4300, 1, 12);
    innerTestForEncodeDecode(4300, 2, 1);
    innerTestForEncodeDecode(0x8000, 4, 127);
}

void ReferenceCompressInt32EncoderTest::TestCaseForUncompress()
{
    innerTestForEncodeDecode(0x8000, 4, 128);
}
IE_NAMESPACE_END(common);

