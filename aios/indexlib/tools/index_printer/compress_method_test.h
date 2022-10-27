#ifndef __INDEXLIB_FLATPOSITIONWRITER_H
#define __INDEXLIB_FLATPOSITIONWRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/storage/file_wrapper.h"

IE_NAMESPACE_BEGIN(index);

enum CompressType {
    CT_VINT = 0,
    CT_P4DELTA = 1,
    CT_NONE = 2
}; 

class CompressMethodTest
{
public:
    CompressMethodTest(uint32_t bufferSize);
    ~CompressMethodTest();
public:
    static const uint32_t minPosCompressNum = 5;
    static const int32_t maxPosNumPerRecord = 128;
    static const int32_t minSkipCompressNum = 10;
    static int64_t BASIC_MASK[];
    static uint32_t bitCount[32];

public:
    void Compress(uint32_t* posBuffer, uint32_t len);

private:
    uint32_t CountBit(uint32_t num, uint8_t compressMethod);
    int32_t P4DeltaCompress(uint32_t* posBuffer, uint32_t len);
    int32_t VintCompress(uint32_t* posBuffer, uint32_t len);
    uint32_t CountBitNum(uint32_t* buffer, uint32_t len, uint8_t compressMethod);
    void PrintCompressSummary();
    void PrintCompressInfo();
    void PrintBitInfo();

private:
    storage::FileWrapperPtr _file;
    char *_dataBuffer;
    uint32_t _bufferSize;
    int64_t _lenBeforeCompress;
    int64_t _lenAfterCompress;
    int64_t _countCallTimes;
    int64_t _vintComCount;
    int64_t _p4deltaComCount;
    int64_t _vintComSize;
    int64_t _p4deltaComSize;
    int64_t _noComCount;
    int64_t _noComSize;
    int64_t _shouldNotComCount;
    int64_t _shouldNotComCSize;
    int64_t _vintTermCount;
    int64_t _p4deltaTermCount;
    int64_t _noCompressTermCount;
    int64_t _shouleNotTermCount;
    int64_t _noNeedCompressSize;
    int64_t _p4deltaGainSize;
    uint32_t _p4deltaBitCount[32];
    uint32_t _groupVarintBitCount[32];
    uint32_t _bitCount[32];
    uint32_t _noCompressBitCount[32];
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressMethodTest);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FLATPOSITIONWRITER_H
