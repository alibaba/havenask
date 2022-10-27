#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/s16_compressor.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(common);

class S16CompressorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(S16CompressorTest);
    void CaseSetUp() override
    {
        const uint32_t SEED = 8888;
        srand(SEED);
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForUInt32()
    {
        for (uint32_t bitNum = 1; bitNum <= 28; ++bitNum)
        {
            InnerTestS16Compressor<uint32_t>(bitNum, 1);
            InnerTestS16Compressor<uint32_t>(bitNum, 30);
            InnerTestS16Compressor<uint32_t>(bitNum, 1000);
        }
    }

    void TestCaseForTooBigUInt32()
    {
        uint32_t data = 1 << 28;
        uint32_t compData;
        INDEXLIB_EXPECT_EXCEPTION(S16Compressor::Encode(&compData, &compData + 1, &data, 1),
                RuntimeException);
    }

    void TestCaseForUInt16()
    {
        for (uint32_t bitNum = 1; bitNum <= 16; ++bitNum)
        {
            InnerTestS16Compressor<uint16_t>(bitNum, 1);
            InnerTestS16Compressor<uint16_t>(bitNum, 30);
            InnerTestS16Compressor<uint16_t>(bitNum, 1000);
        }
    }

    void TestCaseForUInt8()
    {
        for (uint32_t bitNum = 1; bitNum <= 8; ++bitNum)
        {
            InnerTestS16Compressor<uint8_t>(bitNum, 1);
            InnerTestS16Compressor<uint8_t>(bitNum, 30);
            InnerTestS16Compressor<uint8_t>(bitNum, 1000);
        }
    }

    
private:
    template<typename T>
    void InnerTestS16Compressor(uint32_t bitNum, uint32_t itemCount)
    {
        T* data = new T[itemCount];
        uint32_t* compData = new uint32_t[itemCount];
        T* decompData = new T[itemCount];

        uint64_t maxValue = (1 << bitNum);
        for (uint32_t i = 0; i < itemCount; ++i)
        {
            data[i] = (T)(rand() % maxValue);
        }

        uint32_t compLen = S16Compressor::Encode(compData, compData + itemCount, data, itemCount);
        INDEXLIB_TEST_TRUE(compLen <= itemCount);

        uint32_t decompItemCount = S16Compressor::Decode(decompData, decompData + itemCount, 
                compData, compLen);
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

INDEXLIB_UNIT_TEST_CASE(S16CompressorTest, TestCaseForUInt32);
INDEXLIB_UNIT_TEST_CASE(S16CompressorTest, TestCaseForTooBigUInt32);
INDEXLIB_UNIT_TEST_CASE(S16CompressorTest, TestCaseForUInt16);
INDEXLIB_UNIT_TEST_CASE(S16CompressorTest, TestCaseForUInt8);

IE_NAMESPACE_END(common);
