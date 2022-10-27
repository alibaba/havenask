#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/s9_compressor.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);

class S9CompressorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(S9CompressorTest);
    void CaseSetUp() override
    {
        const uint32_t SEED = 8888;
        srand(SEED);
    }

    void CaseTearDown() override
    {
    }

    void TestCaseCalculateBytes()
    {
        uint32_t bits[32] = {1};
        uint8_t data[32] = {1};
        uint32_t dest[32];
        uint32_t count[32] = {0,
                              1, 1, 1, 1, 1, // 1-5
                              2, 1, 2, 1, 2, // 6-10
                              2, 2, 2, 1, 2, // 11-15
                              2, 2, 2, 2, 3, // 16-20
                              2, 3, 2, 3, 3, // 21-25
                              3, 3, 1, 2, 2, 2 // 26-31
                             };
        for (int i = 0; i < 32; ++i)
        {
            INDEXLIB_TEST_EQUAL(4 * count[i], S9Compressor::CalculateBytes(bits, i));
            INDEXLIB_TEST_EQUAL(count[i], S9Compressor::Encode<uint8_t>(dest, 32, data, i));
        }
    }

    void TestCaseForS9EncodeInt8()
    {
        for (uint32_t bitNum = 0; bitNum <=8; ++bitNum)
        {
            InnerTestS9Compressor<uint8_t>(bitNum, 1);
            InnerTestS9Compressor<uint8_t>(bitNum, 30);
            InnerTestS9Compressor<uint8_t>(bitNum, 1000);
        }
    }

    void TestCaseForS9EncodeInt16()
    {
        for (uint32_t bitNum = 0; bitNum <=16; ++bitNum)
        {
            InnerTestS9Compressor<uint16_t>(bitNum, 1);
            InnerTestS9Compressor<uint16_t>(bitNum, 30);
            InnerTestS9Compressor<uint16_t>(bitNum, 1000);
        }
    }

    void TestCaseForS9EncodeInt32()
    {
        for (uint32_t bitNum = 0; bitNum <= 28; ++bitNum)
        {
            InnerTestS9Compressor<uint32_t>(bitNum, 1);
            InnerTestS9Compressor<uint32_t>(bitNum, 30);
            InnerTestS9Compressor<uint32_t>(bitNum, 100);
        }
    }

private:
    
    template<typename T>
    void InnerTestS9Compressor(uint32_t bitNum, uint32_t itemCount)
    {
        T* data = new T[itemCount];
        uint32_t* compData = new uint32_t[itemCount];
        T* decompData = new T[itemCount];

        uint64_t maxValue = (1 << bitNum);
        for (uint32_t i = 0; i < itemCount; ++i)
        {
            data[i] = (T)(rand() % maxValue);
        }

        uint32_t compLen = S9Compressor::Encode(compData, itemCount, 
                data, itemCount);
        INDEXLIB_TEST_TRUE(compLen <= itemCount);

        uint32_t decompItemCount = S9Compressor::Decode(decompData, compData, compLen);
        INDEXLIB_TEST_EQUAL(itemCount, decompItemCount);

        for (uint32_t i = 0; i < itemCount; ++i)
        {
            INDEXLIB_TEST_EQUAL(data[i], decompData[i]);
        }

        delete []data;
        delete []compData;
        delete []decompData;
    }
};

INDEXLIB_UNIT_TEST_CASE(S9CompressorTest, TestCaseCalculateBytes);
INDEXLIB_UNIT_TEST_CASE(S9CompressorTest, TestCaseForS9EncodeInt8);
INDEXLIB_UNIT_TEST_CASE(S9CompressorTest, TestCaseForS9EncodeInt16);
INDEXLIB_UNIT_TEST_CASE(S9CompressorTest, TestCaseForS9EncodeInt32);

IE_NAMESPACE_END(common);
