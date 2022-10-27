#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/new_pfordelta_compressor.h"
#include <time.h>
#include <stdlib.h>

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(common);

class NewPforDeltaCompressorTest : public INDEXLIB_TESTBASE
                                 , public NewPForDeltaCompressor
{
public:
    const static size_t BLOCK_SIZE = NewPForDeltaCompressor::BLOCK_SIZE;


    DECLARE_CLASS_NAME(NewPforDeltaCompressorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForHighBitIdx()
    {
        INDEXLIB_TEST_EQUAL((uint32_t)0, HighBitIdx(0));
        for (size_t i = 0; i < 32; ++i)
        {
            uint32_t value = (1 << i);
            INDEXLIB_TEST_EQUAL(i + 1, HighBitIdx(value));
        }
    }

    template <typename T>
    void TestCaseForCompressBlockWithoutException()
    {
        T block[BLOCK_SIZE];
        for (size_t i = 0; i < BLOCK_SIZE; ++i)
        {
            block[i] = (T)i;
        }
        TestCompressBlock<T>(block, BLOCK_SIZE, 7, 0);
    }

    void TestCaseForCompressInt32BlockWithoutException()
    {
        TestCaseForCompressBlockWithoutException<uint32_t>();
    }

    void TestCaseForCompressInt16BlockWithoutException()
    {
        TestCaseForCompressBlockWithoutException<uint16_t>();
    }

    void TestCaseForCompressInt8BlockWithoutException()
    {
        TestCaseForCompressBlockWithoutException<uint8_t>();
    }

    void TestCaseForCompressBlockWithAllZero()
    {
        uint32_t block[BLOCK_SIZE];
        for (size_t i = 0; i < BLOCK_SIZE; ++i)
        {
            block[i] = (uint32_t)0;
        }
        TestCompressBlock<uint32_t>(block, BLOCK_SIZE, 0, 0);
    }

    void TestCaseForCompressBlockWithAllZeroButOne()
    {
        uint32_t block[BLOCK_SIZE];
        for (size_t i = 0; i < BLOCK_SIZE; ++i)
        {
            block[i] = (uint32_t)0;
        }
        block[7] = 1;
        TestCompressBlock<uint32_t>(block, BLOCK_SIZE, 0, 1);
    }

    void TestCaseForCompressBlockWithException()
    {
        uint32_t block[BLOCK_SIZE];
        size_t i = 0;
        for (; i < BLOCK_SIZE - 2; ++i)
        {
            block[i] = (uint32_t)i;
        }
        const static uint32_t EXP_CODE1 = 129;
        const static uint32_t EXP_CODE2 = 259;
        block[i++] = EXP_CODE1;
        block[i++] = EXP_CODE2;
        TestCompressBlock<uint32_t>(block, BLOCK_SIZE, 7, 2);
    }

    void TestCaseForCompressBlockWithManyException()
    {
        uint32_t block[BLOCK_SIZE];
        size_t i = 0;
        for (; i < BLOCK_SIZE/2; ++i)
        {
            block[i] = (uint32_t)i;
        }
        for (; i < BLOCK_SIZE; ++i)
        {
            block[i] = 129 + (uint32_t)i;
        }
        TestCompressBlock<uint32_t>(block, BLOCK_SIZE, 7, BLOCK_SIZE/2);
    }

    void TestCaseForCompressBlockLessThanBlockSize()
    {
        uint32_t block[BLOCK_SIZE];
        for (size_t i = 1; i < 8; i++)
        {
            for (size_t j = 1; j < BLOCK_SIZE - 1; ++j)
            {
                for (size_t k = 0; k < j; ++k)
                {
                    block[k] = (uint32_t)k * 3;
                    block[k] = ((block[k] << (32 - i)) >> (32 - i));
                }
                TestCompressBlock<uint32_t>(block, j, i, 0);
            }
        }
    }

    void TestCaseForCompressBlockLessThanBlockSizeWithException()
    {
        uint32_t block[BLOCK_SIZE];
        for (size_t i = 1; i < 8; i++)
        {
            for (size_t j = 1; j < BLOCK_SIZE - 1; ++j)
            {
                for (size_t k = 0; k < j; ++k)
                {
                    block[k] = (uint32_t)k * 3;
                    block[k] = ((block[k] << (32 - i)) >> (32 - i));
                }
                for (size_t l = 1; l < j; ++l)
                {
                    size_t inc = j/l;
                    uint32_t block2[BLOCK_SIZE];
                    memcpy(block2, block, j * sizeof(uint32_t));

                    size_t exceptionLimit = (l > BLOCK_SIZE/2) ? BLOCK_SIZE/2 : l;
                    for (size_t pos = 0, num = 0; num < exceptionLimit; pos += inc, ++num)
                    {
                        block2[pos] = block2[pos] | (1 << i);
                    }
                    TestCompressBlock<uint32_t>(block2, j, i, exceptionLimit);
                }
            }
        }
    }

    void TestCaseForS9Encode()
    {
        const static size_t ENCODE_COUNT = 100;
        for (size_t cnt = 0; cnt < ENCODE_COUNT; ++cnt)
        {
            size_t dataSize = ((cnt * 137) % 64) + 1;
            vector<uint32_t> data;
            data.resize(dataSize);
            for (size_t i = 0; i < dataSize; ++i)
            {
                data[i] = i * 137;
                data[i] = (data[i] << 4) >> 4;//make sure less than 28 bit
            }

            for (size_t k = 0; k < dataSize; ++k)
            {
                vector<uint32_t> exceptionPos;
                exceptionPos.push_back(k);
                
                size_t j = 1;
                for (; ; ++j)
                {
                    uint32_t pos = (exceptionPos[j - 1] + ((k * 7)% dataSize) + 1);
                    if (pos >= dataSize)
                        break;
                    exceptionPos.push_back(pos);
                }

                for (int32_t frameBits = 1; frameBits < 30; frameBits++)
                {
                    vector<uint32_t> exceptionData;
                    exceptionData.resize(exceptionPos.size() * 2 - 1);
                    //Remember the answer
                    vector<uint32_t> answer(data.begin(), data.end());
        
                    //Make the exception and exception position array, stored in exceptionData
                    MakeDataForS9Encode(exceptionData, data, exceptionPos, frameBits);
        
                    DoTestS9Decode(answer, exceptionData, data, exceptionPos[0], frameBits);
                }
            }
        }
    }

    void TestCaseForS9EncodeBigValue()
    {
        size_t dataSize = 128;
        vector<uint32_t> data;
        data.resize(dataSize);
        for (size_t i = 0; i < dataSize; ++i)
        {
            data[i] = i;
            if (i == 12)
            {
                data[i] = 1 << 27;
            }
            else if (i == 127)
            {
                data[i] = 1 << (27 + 1);
            }
        }

        vector<uint32_t> exceptionPos;
        exceptionPos.push_back(12);
        exceptionPos.push_back(127);
        vector<uint32_t> exceptionData;
        exceptionData.resize(exceptionPos.size() * 2 - 1);
        //Remember the answer
        vector<uint32_t> answer(data.begin(), data.end());
        //Make the exception and exception position array, stored in exceptionData
        MakeDataForS9Encode(exceptionData, data, exceptionPos, 0);
        INDEXLIB_EXPECT_EXCEPTION(DoTestS9Decode(answer, exceptionData, data, exceptionPos[0], 0),
                                RuntimeException);
    }

    void TestCaseForSelectCompressBitsWithoutException()
    {
        uint32_t block[BLOCK_SIZE];
        for (size_t i = 0; i < 32; ++i)
        {
            for (size_t j = 0; j < BLOCK_SIZE; ++j)
            {
                block[j] = (1 << i) - 1;
            }    
            uint32_t numExceptions = (uint32_t)-1;
            uint32_t frameBits = SelectCompressBits<uint32_t>(block, BLOCK_SIZE, numExceptions);
            INDEXLIB_TEST_EQUAL(FRAME_BIT_MAP[i], frameBits);
            INDEXLIB_TEST_EQUAL((uint32_t)0, numExceptions);
            CheckFrameBitsAndException<uint32_t>(block, BLOCK_SIZE, frameBits, numExceptions);
        }
    }

    void TestCaseForSelectCompressBitsWithoutException2()
    {
#define HELPER(expectBits, expectNumExceptions, expectTotalBytes) do {  \
            uint32_t len = sizeof(block) / sizeof(block[0]);            \
            uint32_t numExceptions;                                     \
            uint32_t frameBits = SelectCompressBits<uint32_t>(block,    \
                    len, numExceptions);                                \
            ASSERT_EQ((uint32_t)expectBits, frameBits);                 \
            ASSERT_EQ((uint32_t)expectNumExceptions, numExceptions);    \
            CheckFrameBitsAndException<uint32_t>(block, len,            \
                    frameBits, numExceptions);                          \
            uint32_t dest[BLOCK_SIZE + 4];                              \
            uint32_t blkCompSize = CompressBlock(dest, BLOCK_SIZE + 4,  \
                    block, len, frameBits, numExceptions, true);        \
            ASSERT_EQ((uint32_t)expectTotalBytes + 4, blkCompSize * 4); \
        } while(0)
        {uint32_t block[] = {4,3,3,3,3,3,3,3,3,3}; HELPER(3, 0, 4);}
        {uint32_t block[] = {4,3,3,3,3,3,3,3,3,3,3}; HELPER(3, 0, 8);}
        {
            uint32_t block[] = {4,3,3,3,3,3,3,3,3,3,
                                3,3,3,3,3,3,3,3,3,1<<27};
            HELPER(3, 1, 12);
        }
        {uint32_t block[] = {11189,3,3,3,3,3,3,3,3,3,3}; HELPER(2, 1, 8);}
        {uint32_t block[] = {1L << 31}; HELPER(32, 0, 4);}
        {uint32_t block[] = {1L << 30}; HELPER(32, 0, 4);}
        {uint32_t block[] = {1L << 23}; HELPER(32, 0, 4);}
        {uint32_t block[] = {1L << 15}; HELPER(32, 0, 4);}
        {uint32_t block[] = {1L << 7}; HELPER(32, 0, 4);}
        {uint32_t block[] = {}; HELPER(0, 0, 0);}
        {
            uint32_t block[] = {15,15,15,15,15,15,15,15,15,15,
                                31,31,31,31,31,31,31,31,31,31}; 
            HELPER(5, 0, 16);
        }
#undef HELPER
    }

    void TestCaseForSelectCompressBitsWithOneException()
    {
        uint32_t block[BLOCK_SIZE];
        for (size_t i = 0; i < 32; ++i)
        {
            for (size_t j = 0; j < BLOCK_SIZE; ++j)
            {
                block[j] = (1 << i) - 1;
            }
            block[i * 7 % BLOCK_SIZE] = (1 << FRAME_BIT_MAP[i]);
            uint32_t numExceptions = (uint32_t)-1;
            uint32_t frameBits = SelectCompressBits<uint32_t>(block, BLOCK_SIZE, numExceptions);
            INDEXLIB_TEST_EQUAL(FRAME_BIT_MAP[i], frameBits);
            if (FRAME_BIT_MAP[i] == 32)
            {
                INDEXLIB_TEST_EQUAL((uint32_t)0, numExceptions);
            }
            else 
            {
                INDEXLIB_TEST_EQUAL((uint32_t)1, numExceptions);
            }
            CheckFrameBitsAndException<uint32_t>(block, BLOCK_SIZE, frameBits, numExceptions);
        }
    }

    void TestCaseForSelectCompressBitsWithManyException()
    {
        uint32_t block[BLOCK_SIZE];
        for (size_t i = 0; i < 32; ++i)
        {
            for (size_t j = 0; j < BLOCK_SIZE; ++j)
            {
                block[j] = (1 << i) - 1;
            }
            block[1] = (1 << FRAME_BIT_MAP[i]);
            block[3] = (1 << FRAME_BIT_MAP[i]);
            block[7] = (1 << FRAME_BIT_MAP[i]);
            uint32_t numExceptions = (uint32_t)-1;
            uint32_t frameBits = SelectCompressBits<uint32_t>(block, BLOCK_SIZE, numExceptions);
            INDEXLIB_TEST_EQUAL(FRAME_BIT_MAP[i], frameBits);

            if (FRAME_BIT_MAP[i] == 32)
            {
                INDEXLIB_TEST_EQUAL((uint32_t)0, numExceptions);
            }
            else 
            {
                INDEXLIB_TEST_EQUAL((uint32_t)3, numExceptions);
            }
            CheckFrameBitsAndException<uint32_t>(block, BLOCK_SIZE, frameBits, numExceptions);
        }
    }

    void TestCaseForSelectCompressBitsWithMaxException()
    {
        size_t maxExceptions = (size_t)((BLOCK_SIZE * EXCEPTION_THRESHOLD_RATE) / 100);
        uint32_t block[BLOCK_SIZE];
        for (size_t i = 0; i < 32; ++i)
        {
            for (size_t j = 0; j < BLOCK_SIZE; ++j)
            {
                block[j] = (1 << i) - 1;
            }

            for (size_t k = 0; k < (size_t)maxExceptions + 1; ++k)
            {
                block[k] = (1 << (i + k));
            }

            uint32_t numExceptions = (uint32_t)-1;
            uint32_t frameBits = SelectCompressBits<uint32_t>(block, BLOCK_SIZE, numExceptions);
            INDEXLIB_TEST_TRUE(numExceptions <= maxExceptions);
            CheckFrameBitsAndException<uint32_t>(block, BLOCK_SIZE, frameBits, numExceptions);
        }
    }

    void TestCaseForSelectCompressBitsWithOnlyOneElem()
    {
        uint32_t block[1];
        block[0] = 11;
        uint32_t numExceptions = (uint32_t)-1;
        uint32_t frameBits = SelectCompressBits<uint32_t>(block, 1, numExceptions);
        INDEXLIB_TEST_EQUAL((uint32_t)0, numExceptions);
        INDEXLIB_TEST_EQUAL((uint32_t)32, frameBits);
    }

    template <typename T>
    void TestCaseForCompressManyBlock()
    {
        const static size_t TEST_COUNT = 100;

        for (size_t i = 0; i < TEST_COUNT; ++i)
        {
            size_t dataSize = (i + 1) * 111;
            vector<T> data;
            data.reserve(dataSize);
            for (size_t i = 0; i < dataSize; ++i)
            {
                data.push_back((T)i % dataSize);
            }
            TestCompress<T>((T*)&data[0], data.size());
        }
    }

    void TestCaseForCompressManyInt32Block()
    {
        TestCaseForCompressManyBlock<uint32_t>();
    }

    void TestCaseForCompressManyInt16Block()
    {
        TestCaseForCompressManyBlock<uint16_t>();
    }

    void TestCaseForCompressManyInt8Block()
    {
        TestCaseForCompressManyBlock<uint8_t>();
    }

    void TestCaseForCompressAndDecompresssWithMultiException()
    {
        const int size = 33;
        uint32_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            src[i] = i + 1;
        }
        src[30] = src[31] = src[32] = 0xFFFF;
        TestCompress<uint32_t>(src, size);
    }

    void TestCaseForCompressAndDecompressWithBigValue()
    {
        const int size = 10000;
        uint32_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            if ( i % 2 == 0) 
            {
                src[i] = i * 987654393;
                src[i] = (src[i] << 4) >> 4;//make sure less than 28 bit
            }
        }
        TestCompress<uint32_t>(src, size);
    }

    void TestCaseForSpecialBlock()
    {
        uint32_t block[128] = 
        {
            359488, 1, 77549, 485071, 484046, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
            2021500, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 5, 1, 1, 1, 1, 1, 1, 1, 
            1, 2019117, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1, 2, 2, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1078828, 167954, 771054, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 278602, 1743230, 1,
            1, 1, 1, 1, 4, 1, 1, 1, 2020139, 1, 1, 3, 1, 1, 1, 1, 2, 1, 1, 1,
            1, 1, 1, 1250309, 17, 772256, 1, 2, 1, 1, 1, 1, 1, 1, 1, 5, 1, 1,
            1, 2, 1248112
        };
        TestCompress<uint32_t>(block, 128);
    }

    void TestCaseForCompressAndDecompressWithExceptionBitsLargerThan28()
    {
        uint32_t block[128] = {0};
        block[126] = 1 << (27 + 1);
        block[6] = 1 << (30 + 1);
        TestCompress<uint32_t>(block, 128);
    }


private:
    template <typename T>
    void TestCompressBlock(const T* src, size_t srcLen, size_t frameBits, size_t numExceptions)
    {
        assert(srcLen <= BLOCK_SIZE);

        uint32_t destBlock[BLOCK_SIZE];
        size_t encodedSize = NewPForDeltaCompressor::CompressBlock<T>(destBlock, 
                BLOCK_SIZE, src, srcLen, frameBits, numExceptions, true);

        bool lastBlock;
        T decodedBlock[BLOCK_SIZE];
        uint32_t* encodePtr = (uint32_t*)destBlock;
        size_t tmpSize = encodedSize;
        size_t decodedSize = NewPForDeltaCompressor::DecompressBlock<T>(decodedBlock,
                BLOCK_SIZE, encodePtr, tmpSize, lastBlock);
        
        INDEXLIB_TEST_EQUAL((size_t)0, tmpSize);
        INDEXLIB_TEST_TRUE((encodePtr - encodedSize) == destBlock);
        INDEXLIB_TEST_EQUAL(srcLen, decodedSize);
        INDEXLIB_TEST_TRUE(lastBlock);

        for (size_t i = 0; i < srcLen; ++i)
        {
            INDEXLIB_TEST_EQUAL(src[i], decodedBlock[i]);
        }
    }

    template <typename T>
    void TestCompress(const T* src, size_t srcLen)
    {
        uint32_t* dest = new uint32_t[srcLen << 1];
        T* decoded = new T[srcLen << 1];
        size_t encodedLen = NewPForDeltaCompressor::Compress<T>(dest, srcLen << 1, src, srcLen);
        
        size_t decodedLen = NewPForDeltaCompressor::Decompress<T>(decoded, srcLen << 1, dest, encodedLen);
        
        INDEXLIB_TEST_EQUAL(srcLen, decodedLen);

        for (size_t i = 0; i < srcLen; ++i)
        {
            INDEXLIB_TEST_EQUAL(src[i], decoded[i]);
        }

        delete[] dest;        
        delete[] decoded;        
    }

    void MakeDataForS9Encode(vector<uint32_t>& exceptionData, vector<uint32_t>& data,
                           const vector<uint32_t>& exceptionPos, int32_t frameBits)
    {
        uint32_t cur = exceptionPos[0];
        uint32_t val = data[cur];
        exceptionData[0] = (val >> frameBits);
        data[cur] = (val & BASIC_MASK[frameBits]);

        uint32_t pre = cur;
        for (size_t i = 1; i < exceptionPos.size(); ++i)
        {
            cur = exceptionPos[i];
            val = data[cur];
            exceptionData[i * 2 - 1] = cur - pre - 1;
            exceptionData[i * 2] = (val >> frameBits);

            data[cur] = (val & BASIC_MASK[frameBits]);
            pre = cur;
        }
    }
    
    void DoTestS9Decode(const vector<uint32_t>& answer, const vector<uint32_t>& data, 
                        const vector<uint32_t>& encode, size_t firstExceptionPos,
                        int32_t frameBits)
    {
        vector<uint32_t> dest;
        dest.resize(data.size());
        vector<uint32_t> decoded = encode;

        size_t encodeLen = S9Encode(&(dest[0]), data.size(), (uint32_t*)&(data[0]), data.size());
        S9Decode(&(decoded[0]), &(dest[0]), encodeLen, firstExceptionPos, frameBits);
        
        for (size_t i = 0; i < answer.size(); i++)
        {
            INDEXLIB_TEST_EQUAL(answer[i], decoded[i]);
        }
    }

    template <typename T>
    void CheckFrameBitsAndException(const T* block, size_t blkLen, uint32_t frameBits, uint32_t numExceptions)
    {
        size_t expectedExp = 0;
        for (size_t i = 0; i < blkLen; ++i)
        {
            if (((uint64_t)block[i] >> frameBits) != 0)
            {
                expectedExp++;
            }
        }
        INDEXLIB_TEST_EQUAL(expectedExp, numExceptions);
    }
};

INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForHighBitIdx);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressInt32BlockWithoutException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressInt16BlockWithoutException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressInt8BlockWithoutException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressBlockWithAllZero);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressBlockWithAllZeroButOne);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressBlockWithException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressBlockWithManyException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressBlockLessThanBlockSize);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressBlockLessThanBlockSizeWithException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForS9Encode);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForS9EncodeBigValue);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressManyInt32Block);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressManyInt16Block);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressManyInt8Block);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForSelectCompressBitsWithoutException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForSelectCompressBitsWithoutException2);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForSelectCompressBitsWithOneException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForSelectCompressBitsWithManyException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForSelectCompressBitsWithMaxException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForSelectCompressBitsWithOnlyOneElem);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressAndDecompresssWithMultiException);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressAndDecompressWithBigValue);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForSpecialBlock);
INDEXLIB_UNIT_TEST_CASE(NewPforDeltaCompressorTest, TestCaseForCompressAndDecompressWithExceptionBitsLargerThan28);

IE_NAMESPACE_END(common);
