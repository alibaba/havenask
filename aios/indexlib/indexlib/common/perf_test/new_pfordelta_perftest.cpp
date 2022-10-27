#include <iostream>
#include <sys/time.h>
#include <fstream>
#include <string>
#include <vector>
#include "indexlib/common/numeric_compress/pfor_delta_compressor.h"
#include "indexlib/common/numeric_compress/new_pfordelta_compressor.h"

using namespace std;
using namespace std::tr1;

IE_NAMESPACE_USE(common);

class CompressorWrapper
{
public:
    template<typename T>
    static size_t compress(uint32_t *dest, size_t destLen,
                           const T* src, size_t srcLen)
    {
        assert(false);
        return 0;
    }

    template<typename T>
    static size_t decompress(T* dest, size_t destLen,
                             const uint32_t *src, size_t srcLen)
    {
        assert(false);
        return 0;
    }
    
private:
    static NewPForDeltaCompressor mNewPFDCompressor;
};

NewPForDeltaCompressor CompressorWrapper::mNewPFDCompressor;

template<>
inline size_t CompressorWrapper::compress(uint32_t *dest, size_t destLen,
        const uint8_t* src, size_t srcLen)
{
    return mNewPFDCompressor.CompressInt8(dest, destLen, src, srcLen);
}

template<>
inline size_t CompressorWrapper::compress(uint32_t *dest, size_t destLen,
        const uint16_t* src, size_t srcLen)
{
    return mNewPFDCompressor.CompressInt16(dest, destLen, src, srcLen);
}

template<>
inline size_t CompressorWrapper::compress(uint32_t *dest, size_t destLen,
        const uint32_t* src, size_t srcLen)
{
    return mNewPFDCompressor.CompressInt32(dest, destLen, src, srcLen);
}

template<>
inline size_t CompressorWrapper::decompress(uint8_t* dest, size_t destLen,
        const uint32_t *src, size_t srcLen)
{
    return mNewPFDCompressor.DecompressInt8(dest, destLen, src, srcLen);
}

template<>
inline size_t CompressorWrapper::decompress(uint16_t* dest, size_t destLen,
        const uint32_t *src, size_t srcLen)
{
    return mNewPFDCompressor.DecompressInt16(dest, destLen, src, srcLen);
}

template<>
inline size_t CompressorWrapper::decompress(uint32_t* dest, size_t destLen,
        const uint32_t *src, size_t srcLen)
{
    return mNewPFDCompressor.DecompressInt32(dest, destLen, src, srcLen);
}

void printUsage()
{
    cout << "Usage: \n"  
         << "new_pfordelta_perftest input_file bit_size memory_limit_MB\n"
         << "example:\n"
         << "\tnew_pfordelta_perftest ./docid_list 32 1024\n"
         << "\tnew_pfordelta_perftest ./docpayload 16 2048\n"        
         << endl;
}

uint64_t GetCurrentTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((uint64_t)tv.tv_sec * 1000000 + tv.tv_usec);
}

template<typename T>
uint64_t readData(const char *file, T* buffer, uint64_t sizeLimit,
                  vector<uint32_t>& recordSize)
{
    freopen(file, "r", stdin);
    T v = 0;
    char c = 0;
    uint64_t cursor = 0;
    uint32_t size = 0;
    while ((c = getchar()) != EOF)
    {
        if (c == ' ')
        {
            buffer[cursor++] = v;
            v = 0;
            size++;
        }
        else
        {
            v = v * 10 + c - '0';
        }
        if (cursor == sizeLimit)
        {
            recordSize.push_back(size);
            return cursor;
        }
        if (c == '\n')
        {
            recordSize.push_back(size);
            size = 0;
            v = 0;
        }
    }
    return cursor;
}

template<typename T>
uint64_t doCompress(const T* src, uint32_t* dest,
                    const vector<uint32_t> &recordSize,
                    vector<uint32_t> &compressedSize)
{
    uint64_t srcOffset = 0;
    uint64_t destOffset = 0;
    for (uint32_t i = 0; i < recordSize.size(); ++i)
    {
        uint32_t size = CompressorWrapper::compress(dest + destOffset,
                recordSize[i] * 2, src + srcOffset, recordSize[i]);
        srcOffset += recordSize[i];
        destOffset += size;
        compressedSize[i] = size;
    }
    return destOffset;
}

template<typename T>
void doDecompress(uint32_t *src, T* dest, const vector<uint32_t> &compressedSize)
{
    uint64_t srcOffset = 0;
    for (uint32_t i = 0; i < compressedSize.size(); ++i)
    {
        CompressorWrapper::decompress(dest, sizeof(dest),
                src + srcOffset, compressedSize[i]);
        srcOffset += compressedSize[i];
    }
}

template<typename T>
void doTest(const char *file, uint64_t memoryLimit)
{
    uint64_t dataCountLimit = memoryLimit / sizeof(T);
    T *dataBuffer = new T[dataCountLimit];
    uint32_t *compressedBuffer = (uint32_t *)new T[dataCountLimit];
    for (uint64_t i = 0; i < memoryLimit / 4; ++i)
    {
        compressedBuffer[i] = i;
    }
    vector<uint32_t> recordSize;
    vector<uint32_t> compressedRecordSize;

    uint64_t originalSize = readData(file, dataBuffer, dataCountLimit,
            recordSize) * sizeof(T);
    compressedRecordSize.resize(recordSize.size());
    cout << "read finished, originalSize: " << originalSize / 1024.0 / 1024.0
         << " MB, recordSize: " << recordSize.size() << endl;

    uint32_t compressCount = 20;
    uint32_t decompressCount = 100;
    uint64_t compressedSize = 0;
    
    uint64_t startTime = GetCurrentTime();
    for (uint32_t i = 0; i < compressCount; ++i)
    {
        compressedSize = doCompress(dataBuffer, compressedBuffer,
                recordSize, compressedRecordSize) * sizeof(uint32_t);
    }
    uint64_t endTime = GetCurrentTime();
    double totalTime = (endTime - startTime) / 1000.0 / 1000.0;

    cout << "compress ratio: " << 100.0 * compressedSize / originalSize << "%"
         << endl << "compress speed: "
         << originalSize * compressCount / totalTime / 1024.0 / 1024.0 << " MB/s"
         << endl;

    startTime = GetCurrentTime();
    for (uint32_t i = 0; i < decompressCount; ++i)
    {
        doDecompress(compressedBuffer, dataBuffer, compressedRecordSize);   
    }
    endTime = GetCurrentTime();
    totalTime = (endTime - startTime) / 1000.0 / 1000.0;
    cout << "decompress speed: "
         << originalSize * decompressCount / totalTime / 1024.0 / 1024.0 << " MB/s"
         << endl;
}

int main(int argc, char** argv) 
{
    if (argc != 4)
    {
        printUsage();
        return 0;
    }
    char *inputFile = argv[1];
    uint32_t bitSize = 0;
    sscanf(argv[2], "%u", &bitSize);
    
    uint64_t memoryLimit = 0;
    sscanf(argv[3], "%lu", &memoryLimit);
    memoryLimit *= 1024 * 1024 / 2; // half for src, half for dest
    
    if (bitSize == 8)
    {
        doTest<uint8_t>(inputFile, memoryLimit);
    }
    else if (bitSize == 16)
    {
        doTest<uint16_t>(inputFile, memoryLimit);
    }
    else if (bitSize == 32)
    {
        doTest<uint32_t>(inputFile, memoryLimit);
    }
    else
    {
        cout << "only support int8, int16, int32" << endl;
    }
    return 0;
}
