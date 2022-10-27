#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/pfor_delta_compressor.h"
#include "indexlib/common/byte_slice_writer.h"
#include <iostream>

using namespace std;
IE_NAMESPACE_BEGIN(common);

class PforDeltaCompressorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(PforDeltaCompressorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForCompressWithOneBlock()
    {
        const int size = 10;
        uint32_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            src[i] = i + 1;
        }
        src[size - 1] = 0xFFFF;
        
        PforDeltaCompressor compressor;
        uint8_t res[size * 8] = {0};
        char* buffer = (char*)res;
        uint32_t answer[] = 
            { 0x0901040a, 
              0x87654321, 0x0009,
              0x3FFFF
            };

        int len = compressor.Compress32(res, sizeof(res), src, size);
        INDEXLIB_TEST_EQUAL(15, len);

        for (int i = 0; 4 * i < len; i += 1)
        {
            INDEXLIB_TEST_EQUAL(answer[i], *(uint32_t*)(buffer + (i * 4)));
        }        
    }

    void TestCaseForCompressWithoutException()
    {
        const int size = 10;
        uint32_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            src[i] = i + 1;
        }
                
        PforDeltaCompressor compressor;
        uint8_t res[size * 8];
	char* buffer = (char*)res;
	
        uint32_t answer[] = 
            { 0x8000040a, 
              0x87654321, 0x000a9
            };

        int len = compressor.Compress32(res, sizeof(res), src, size);
        INDEXLIB_TEST_EQUAL((int)(sizeof(answer)/ sizeof(uint8_t)), len);

        for (int i = 0; 4 * i < len; i += 1)
        {
            INDEXLIB_TEST_EQUAL(answer[i], *(uint32_t*)(buffer + (i * 4)));
        }        
    }
    
    void TestCaseForCompressWithMultipleBlock()
    {
        const int size = 128;
        const int blockNum = 2;
        uint32_t blockSrc[size];
        for(int i = 0; i < size; ++i)
        {
            blockSrc[i] = i + 50;
        }
        blockSrc[size - 1] = 0xffff;
        
        uint32_t src[size * blockNum];
        memcpy(src, blockSrc, sizeof(blockSrc));
        memcpy(src + size, blockSrc, sizeof(blockSrc));
        
        PforDeltaCompressor compressor;

        uint8_t answer[sizeof(src) * 2] ;
        answer[0] = size;
        answer[1] = 8;
        answer[2] = 1;
        answer[3] = 127;
        for(int i = 0; i < size; ++i)
        {
            answer[4+i] = blockSrc[i];
        }
        answer[size + 3] = 0;//TODO last exception in codes section value unchecked
        answer[size + 4] = 0xff;
        answer[size + 5] = 0xff;
        answer[size + 6] = 3;
        //answer[size + 7] = 0;
            
        uint8_t* answer2 = answer + size + 7;
        memcpy(answer2, answer, size + 7);
        
        uint8_t res[sizeof(answer)];
        int len = compressor.Compress32(res, sizeof(res), src, size * 2);
        INDEXLIB_TEST_EQUAL((size+7) * 2, len);
        INDEXLIB_TEST_TRUE(memcmp(res, answer, len) == 0);
    }

    void TestCaseForDecompressWithOneBlock()
    {
        uint32_t compressData[]= { 0x0901040a, 0x87654321, 0x0009, 0x3FFFF};
        uint32_t result[PforDeltaCompressor::PFOR_DELTA_BLOCK];
        uint32_t answer[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xFFFF};
        
        PforDeltaCompressor compressor;
        int32_t len = compressor.Decompress32(result, sizeof(result)/sizeof(uint32_t), 
                (uint8_t*)compressData, sizeof(compressData) - 1);
        INDEXLIB_TEST_EQUAL((int32_t)(sizeof(answer) / sizeof(uint32_t)), len);

        for (int i = 0; i < len; i++)
        {
            INDEXLIB_TEST_EQUAL(answer[i], result[i]);
        }
    }

    void TestCaseForDecompressMultipleBlock()
    {
        const int size = 128;
        const int blockNum = 2;
        uint32_t block[size];
        for(int i = 0; i < size; ++i)
        {
            block[i] = i + 50;
        }
        block[size - 1] = 0xffff;
        
        uint32_t answer[size * blockNum];
        memcpy(answer, block, sizeof(block));
        memcpy(answer + size, block, sizeof(block));
                
        uint8_t compressData[sizeof(answer) * 2] ;
        compressData[0] = size;
        compressData[1] = 8;
        compressData[2] = 1;
        compressData[3] = 127;
        for(int i = 0; i < size; ++i)
        {
            compressData[4+i] = block[i];
        }
        compressData[size + 3] = 0;//TODO last exception in codes section value unchecked
        compressData[size + 4] = 0xff;
        compressData[size + 5] = 0xff;
        compressData[size + 6] = 3;
//        compressData[size + 7] = 0;

        uint8_t* compressData2 = compressData + size + 7;
        memcpy(compressData2, compressData, size + 7);
        
        uint32_t result[sizeof(answer) * 2];
        PforDeltaCompressor compressor;
        int len = compressor.Decompress32(result, sizeof(result)/sizeof(uint32_t), 
                (uint8_t*)compressData, 2 * (size + 7));
        
        INDEXLIB_TEST_EQUAL((int)(sizeof(answer)/sizeof(uint32_t)), len);
        for (int i = 0; i < len; ++i)
        {
            INDEXLIB_TEST_EQUAL(answer[i], result[i]);
        }
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
                
        PforDeltaCompressor compressor;
        uint8_t res[size * 8];

        int compressLen = compressor.Compress32(res, sizeof(res), src, size);
        INDEXLIB_TEST_EQUAL(37, compressLen);

        uint32_t decompressBuf[size];
        int decompressNum = compressor.Decompress32(decompressBuf, sizeof(decompressBuf), res, compressLen);
        INDEXLIB_TEST_EQUAL(size, decompressNum);
        
        for (int i = 0; i < size; i++)
        {
            INDEXLIB_TEST_EQUAL(src[i], decompressBuf[i]);
        }        
    }


    void TestCaseForCompressAndDecompressWithManyBlock()
    {
        const int size = 10000;
        uint32_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            if ( i % 2 == 0) {
                src[i] = i * 987654393;
            }

        }
                
        PforDeltaCompressor compressor;
        uint8_t res[size * 16];

        int compressLen = compressor.Compress32(res, sizeof(res), src , size );
        INDEXLIB_TEST_TRUE(compressLen > 0);
        uint32_t decompressBuf[size];
        int decompressNum = compressor.Decompress32(decompressBuf, sizeof(decompressBuf), res, compressLen);
        INDEXLIB_TEST_EQUAL(size, decompressNum);
        
        for (int i = 0; i < size; i++)
        {
            INDEXLIB_TEST_EQUAL(src[i], decompressBuf[i]);
        }        
    }
    
    void TestCaseForDecompressBlockFromOneBlock()
    {
        uint32_t compressData[]= { 0x0901040a, 0x87654321, 0x0009, 0x3FFFF};
        uint32_t result[PforDeltaCompressor::PFOR_DELTA_BLOCK];
        uint32_t answer[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0xFFFF};
        
        PforDeltaCompressor compressor;
        size_t blockLen = 0;
        int32_t len = compressor.DecompressBlock32(result, sizeof(result)/sizeof(uint32_t), 
                (uint8_t*)compressData, sizeof(compressData) - 1, blockLen);
        INDEXLIB_TEST_EQUAL((int32_t)(sizeof(answer) / sizeof(uint32_t)), len);
        INDEXLIB_TEST_EQUAL(blockLen, sizeof(compressData) - 1);
        for (int i = 0; i < len; i++)
        {
            INDEXLIB_TEST_EQUAL(answer[i], result[i]);
        }


    }

    void TestCaseForDecompressBlockFromManyBlock()
    {
        const int size = 10000;
        uint32_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            if ( i % 2 == 0) {
                src[i] = i * 987654393;
            }

        }
                
        PforDeltaCompressor compressor;
        uint8_t res[size * 16];

        int compressLen = compressor.Compress32(res, sizeof(res), src , size );
        INDEXLIB_TEST_TRUE(compressLen > 0);
        uint32_t decompressBuf[size];
        int totalDecompressedSrcLen = 0;
        int totalDecompressedLen = 0;
        while(totalDecompressedSrcLen < compressLen)
        {
            size_t blockLen = 0;
            ssize_t decomLen = compressor.DecompressBlock32(decompressBuf + totalDecompressedLen, 
                    size - totalDecompressedLen, res + totalDecompressedSrcLen, compressLen - totalDecompressedSrcLen, blockLen);
            totalDecompressedSrcLen += blockLen;
            INDEXLIB_TEST_TRUE(!(decomLen ==  -1));
            totalDecompressedLen += decomLen;
        }
        INDEXLIB_TEST_EQUAL(size, totalDecompressedLen);

        for (int i = 0; i < size; i++)
        {
            INDEXLIB_TEST_EQUAL(src[i], decompressBuf[i]);
        }        
    }

    
    void TestCaseForCompress16AndDecompress16WithManyBlock()
    {
        srand(time(NULL));
        const int size = 20000;
        const uint16_t maxNum = 65535;
        uint16_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            if ( i % 2 == 0) 
            {
                src[i] = rand() % maxNum;
            }
            else 
            {
                src[i] = i;
            }
        }

        PforDeltaCompressor compressor;
        uint8_t res[size * 16];

        int compressLen = compressor.Compress16(res, sizeof(res), src , size );
        INDEXLIB_TEST_TRUE(compressLen > 0);
        uint16_t decompressBuf[size];
        int decompressNum = compressor.Decompress16(decompressBuf, sizeof(decompressBuf), res, compressLen);
        INDEXLIB_TEST_EQUAL(size, decompressNum);
        
        for (int i = 0; i < size; i++)
        {
            INDEXLIB_TEST_EQUAL(src[i], decompressBuf[i]);
        }
    }
    
    void TestCaseForCompress16AndDecompress16_SliceReader()
    {
        const int size = 127;
        uint16_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            if ( i % 2 == 0) 
            {
                src[i] = (uint8_t)(i * 987653397);
            }
            else 
            {
                src[i] = i;
            }
        }
        
        PforDeltaCompressor compressor;
        uint8_t res[size * 16];
        int compressLen = compressor.Compress16(res, sizeof(res), src , size );
        INDEXLIB_TEST_TRUE(compressLen > 0);
//        cout << "compressLen: " << compressLen << endl;

        ByteSliceWriter writer;
        for (int i = 0; i < compressLen; ++i)
        {
            writer.WriteByte(res[i]);
        }

        ByteSliceReader reader(writer.GetByteSliceList());

        uint16_t decompressBuf[size];
        ssize_t decompressNum = PforDeltaCompressor::DecompressBlock16(decompressBuf, size, reader);
        INDEXLIB_TEST_EQUAL((ssize_t)size, decompressNum);
        for (int i = 0; i < size; i++)
        {
            INDEXLIB_TEST_EQUAL(src[i], decompressBuf[i]);
        }        
    }


    void TestCaseForCompress8AndDecompress8WithManyBlock()
    {
        const int size = 10000;
        uint8_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            if ( i % 2 == 0) {
                src[i] = (uint8_t)(i * 987654393);
            }
        }
                
        PforDeltaCompressor compressor;
        uint8_t res[size * 16];

        int compressLen = compressor.Compress8(res, sizeof(res), src , size );
        INDEXLIB_TEST_TRUE(compressLen > 0);
        uint8_t decompressBuf[size];
        int decompressNum = compressor.Decompress8(decompressBuf, sizeof(decompressBuf), res, compressLen);
        INDEXLIB_TEST_EQUAL(size, decompressNum);
        
        for (int i = 0; i < size; i++)
        {
            INDEXLIB_TEST_EQUAL(src[i], decompressBuf[i]);
        }        
    }

    void TestCaseForCompress8AndDecompress8_SliceReader()
    {
        const int size = 127;
        uint8_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            if ( i % 2 == 0) 
            {
                src[i] = (uint8_t)(i * 987653397);
            }
            else 
            {
                src[i] = i;
            }
        }
        
        PforDeltaCompressor compressor;
        uint8_t res[size * 16];
        int compressLen = compressor.Compress8(res, sizeof(res), src , size );
        INDEXLIB_TEST_TRUE(compressLen > 0);
//        cout << "compressLen: " << compressLen << endl;

        ByteSliceWriter writer;
        for (int i = 0; i < compressLen; ++i)
        {
            writer.WriteByte(res[i]);
        }

        ByteSliceReader reader(writer.GetByteSliceList());

        uint8_t decompressBuf[size];
        ssize_t decompressNum = PforDeltaCompressor::DecompressBlock8(decompressBuf, size, reader);
        INDEXLIB_TEST_EQUAL((ssize_t)size, decompressNum);
        for (int i = 0; i < size; i++)
        {
            INDEXLIB_TEST_EQUAL(src[i], decompressBuf[i]);
        }        

    }

    void TestCaseForCompress8AndDecompress8_SliceReader_ManyBlock()
    {
        const int size = 10000;
        uint8_t src[size] = {0};
        for (int i = 0; i < size; ++i)
        {
            if ( i % 2 == 0) 
            {
                src[i] = (uint8_t)(i * 987653397);
            }
            else 
            {
                src[i] = i;
            }
        }
        
        PforDeltaCompressor compressor;
        uint8_t res[size * 16];
        int compressLen = compressor.Compress8(res, sizeof(res), src , size );
        INDEXLIB_TEST_TRUE(compressLen > 0);
//        cout << "compressLen: " << compressLen << endl;

        ByteSliceWriter writer;
        for (int i = 0; i < compressLen; ++i)
        {
            writer.WriteByte(res[i]);
        }

        ByteSliceReader reader(writer.GetByteSliceList());

        uint8_t decompressBuf[size];
        int totalSize = 0;
        while (totalSize < size)
        {
            ssize_t decompressNum = PforDeltaCompressor::DecompressBlock8(decompressBuf, size, reader);
            INDEXLIB_TEST_TRUE(decompressNum > 0);
            if (size - totalSize >= 128)
            {
                uint32_t pforDeltaBlock = PforDeltaCompressor::PFOR_DELTA_BLOCK;
                INDEXLIB_TEST_EQUAL(pforDeltaBlock, decompressNum);
            }
            else 
            {
                INDEXLIB_TEST_EQUAL(size - totalSize, decompressNum);
            }
            for (int i = 0; i < decompressNum; i++)
            {
                INDEXLIB_TEST_EQUAL(src[totalSize + i], decompressBuf[i]);
            }
            totalSize += decompressNum;
        }
        

    }


private:
};

INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompressWithOneBlock);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompressWithoutException);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompressWithMultipleBlock);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForDecompressWithOneBlock);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForDecompressMultipleBlock);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompressAndDecompresssWithMultiException);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompressAndDecompressWithManyBlock);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForDecompressBlockFromOneBlock);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForDecompressBlockFromManyBlock);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompress16AndDecompress16WithManyBlock);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompress16AndDecompress16_SliceReader);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompress8AndDecompress8WithManyBlock);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompress8AndDecompress8_SliceReader);
INDEXLIB_UNIT_TEST_CASE(PforDeltaCompressorTest, TestCaseForCompress8AndDecompress8_SliceReader_ManyBlock);

IE_NAMESPACE_END(common);
