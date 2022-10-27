#include "tools/index_printer/compress_method_test.h"
#include "indexlib/common/numeric_compress/pfor_delta_compressor.h"
#include "indexlib/common/numeric_compress/new_pfordelta_compressor.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/common/numeric_compress/group_varint.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(tools, CompressMethodTest);

int64_t CompressMethodTest::BASIC_MASK[] = 
{
    1,
    ( 1 << 2 )  - 1,
    ( 1 << 3 )  - 1,
    ( 1 << 4 )  - 1,
    ( 1 << 5 )  - 1,
    ( 1 << 6 )  - 1,
    ( 1 << 7 )  - 1,
    ( 1 << 8 )  - 1,
    ( 1 << 9 )  - 1,
    ( 1 << 10 ) - 1,
    ( 1 << 11 ) - 1,
    ( 1 << 12 ) - 1,
    ( 1 << 13 ) - 1,
    ( 1 << 14 ) - 1,
    ( 1 << 15 ) - 1,
    ( 1 << 16 ) - 1,
    ( 1 << 17 ) - 1,
    ( 1 << 18 ) - 1,
    ( 1 << 19 ) - 1,
    ( 1 << 20 ) - 1,
    ( 1 << 21 ) - 1,
    ( 1 << 22 ) - 1,
    ( 1 << 23 ) - 1,
    ( 1 << 24 ) - 1,
    ( 1 << 25 ) - 1,
    ( 1 << 26 ) - 1,
    ( 1 << 27 ) - 1,
    ( 1 << 28 ) - 1,
    ( 1 << 29 ) - 1,
    ( 1 << 30 ) - 1,
    ( (int64_t)1 << 31 ) - 1,
    ( (int64_t)1 << 32 ) - 1
};


CompressMethodTest::CompressMethodTest(uint32_t bufferSize)
{
    _bufferSize = bufferSize << 2;
    _dataBuffer = NULL;
    _lenBeforeCompress = 0;
    _lenAfterCompress = 0;
    _countCallTimes = 0;
    _vintComCount = 0;
    _p4deltaComCount = 0;
    _vintComSize = 0;
    _p4deltaComSize = 0;
    _noComCount = 0;
    _noComSize = 0;
    _shouldNotComCount = 0;
    _shouldNotComCSize = 0;
    _vintTermCount = 0;
    _p4deltaTermCount = 0;
    _noCompressTermCount = 0;
    _shouleNotTermCount = 0;
    _noNeedCompressSize = 0;
    _p4deltaGainSize = 0;
    memset(_p4deltaBitCount, 0, sizeof(_p4deltaBitCount));
    memset(_groupVarintBitCount, 0, sizeof(_groupVarintBitCount));
    memset(_bitCount, 0, sizeof(_bitCount));
    memset(_noCompressBitCount, 0, sizeof(_noCompressBitCount));
}

CompressMethodTest::~CompressMethodTest() 
{
    PrintCompressSummary();
    //PrintBitInfo();
    PrintCompressInfo();
 
    delete []_dataBuffer;
    _dataBuffer = NULL;
}

void CompressMethodTest::PrintCompressSummary()
{
    int64_t totalComCount = _vintComCount + _p4deltaComCount + _noComCount;
    IE_LOG(ERROR, "avg data compress ratio [%f](%ld / %ld) ",
           (float)_lenAfterCompress/_lenBeforeCompress, 
           _lenAfterCompress, _lenBeforeCompress);
    IE_LOG(ERROR, "totalComCount: %ld vintComCount: %ld p4ComCount: %ld "
           "noComCount: %ld\n"
           "and the ratio is vinComCount: %f p4ComCount: %f noComCount: %f",
           totalComCount,_vintComCount, _p4deltaComCount, _noComCount,
           (float)_vintComCount / totalComCount,
           (float)_p4deltaComCount / totalComCount,
           (float)_noComCount / totalComCount);
}

void CompressMethodTest::PrintCompressInfo()
{
    IE_LOG(ERROR, "GVCompressSize:%ld  p4deltaCompressSize:%ld"
           " noCompressSize:%ld  shouldNotCompressSize:%ld",
	   _vintComSize, _p4deltaComSize, _noComSize, _shouldNotComCSize);
    IE_LOG(ERROR, "GVCopressTermCount:%ld, p4deltaCompressTermCount:%ld"
           " noCompressTermCount:%ld shouldNotCompressTermCount:%ld", 
           _vintTermCount, _p4deltaTermCount, _noCompressTermCount, 
           _shouleNotTermCount);

    IE_LOG(ERROR, "countCallTimes = %ld", _countCallTimes);

    IE_LOG(ERROR, "GVCompressSizeRatio:%f  p4deltaCompressSizeRatio:%f"
           " noCompressSizeRatio:%f  shouldNotCompressSizeRatio:%f",
           (float)_vintComSize/_lenAfterCompress, 
           (float)_p4deltaComSize/_lenAfterCompress,
           (float) _noComSize/_lenAfterCompress,
           (float) _shouldNotComCSize/_lenAfterCompress);

    IE_LOG(ERROR, "GVCopressTermRatio:%f, p4deltaCompressTermRatio:%f"
           " noCompressTermRatio:%f shouldNotCompressTermRatio:%f", 
           (float)_vintTermCount/_countCallTimes, 
           (float)_p4deltaTermCount/_countCallTimes,
           (float) _noCompressTermCount/_countCallTimes,
           (float) _shouleNotTermCount/_countCallTimes);

    IE_LOG(ERROR, "p4deltaGain size:%ld, no need compress size:%ld", 
           _p4deltaGainSize, _noNeedCompressSize);

}

void CompressMethodTest::PrintBitInfo()
{
    IE_LOG(ERROR, "Global bits info");
    for(int i = 0; i < 32; i++) 
    {
        IE_LOG(ERROR, "data take bit count: %d bits, count %u ",
               i+1, _bitCount[i]);
    }
    IE_LOG(ERROR, "GroupVarint compress bits info");
    for(int i = 0; i < 32; i++) 
    {
        IE_LOG(ERROR, "data take bit count: %d bits, count %u ",
               i+1, _groupVarintBitCount[i]);
    }

    uint64_t sumBitCount = 0;
    for(uint64_t i = 0; i < 32; i++) 
    {
        sumBitCount += ((i + 1) * _groupVarintBitCount[i]);
    }
    uint64_t byteCount = (sumBitCount -1) / 8 + 1;
    IE_LOG(ERROR, "min bytes:%ld compressBytes:%ld", byteCount,_vintComSize);

    IE_LOG(ERROR, "pfordelta compress bits info");
    for(int i = 0; i < 32; i++) 
    {
        IE_LOG(ERROR, "data take bit count: %d bits, count %u ",
               i+1, _p4deltaBitCount[i]);
    }
    
    sumBitCount = 0;
    for(uint64_t i = 0; i < 32; i++) 
    {
        sumBitCount += ((i + 1) * _p4deltaBitCount[i]);
    }
    byteCount = (sumBitCount -1) / 8 + 1;
    IE_LOG(ERROR, "min bytes:%ld compressBytes:%ld", byteCount,_p4deltaComSize);

    IE_LOG(ERROR, "no compress bits info");
    for(int i = 0; i < 32; i++) 
    {
        IE_LOG(ERROR, "data take bit count: %d bits, count %u ",
               i+1, _noCompressBitCount[i]);
    }

    sumBitCount = 0;
    for(uint64_t i = 0; i < 32; i++) 
    {
        sumBitCount += ((i + 1) * _noCompressBitCount[i]);
    }
    byteCount = (sumBitCount -1) / 8 + 1;
    IE_LOG(ERROR, "min bytes:%ld compressBytes:%ld", byteCount,_noComSize);

}
uint32_t CompressMethodTest::CountBit(uint32_t num, uint8_t compressMothed) {
    for(int i = 30; i >= 0; i--) {
        if ((int64_t)num > BASIC_MASK[i]) {
            int id = i + 1;
            _bitCount[id]++;
            if (compressMothed == CT_VINT)
            {
                _groupVarintBitCount[id]++;
            }
            else if (compressMothed == CT_P4DELTA)
            {
                _p4deltaBitCount[id]++;
            }
            else if (compressMothed == CT_NONE)
            {
                _noCompressBitCount[id]++;
            }
            return id;
        }
    }
    //when num is 0 or 1, it need 1 bit
    _bitCount[0]++;
    if (compressMothed == CT_VINT)
    {
        _groupVarintBitCount[0]++;
    }
    else if (compressMothed == CT_P4DELTA)
    {
        _p4deltaBitCount[0]++;
    }
    else if (compressMothed == CT_NONE)
    {
        _noCompressBitCount[0]++;
    }
    return 0;
}

uint32_t CompressMethodTest::CountBitNum(uint32_t* buffer, uint32_t len, uint8_t compressMethod)
{
    uint32_t blockBitCount[32] = {0};
    int id = 0;
    for (uint32_t i = 0; i < len; i++) 
    {
        id = CountBit(buffer[i], compressMethod);
        ++blockBitCount[id];
    }

    stringstream ss;
    uint32_t sumBitCount = 0;
    for(int i = 0; i < 32; i++) 
    {
        sumBitCount += ((i + 1) * blockBitCount[i]);
        ss << blockBitCount[i] << " ";
    }
    uint32_t avgByteCount = (sumBitCount % 8 == 0)? sumBitCount / 8 : (sumBitCount / 8 + 1);
    ss << "sumBitCount:" << sumBitCount << " ByteCount:" << avgByteCount ;
    
    if (compressMethod == CT_VINT)
    {
        ss << " GVarint";
    }
    else if (compressMethod == CT_P4DELTA)
    {
        ss << " pfor_delta";
    }
    else
    {
        ss << " none";
    }
    ss << endl;
    //IE_LOG(ERROR, "%s", ss.str().c_str());
    return avgByteCount;
}

int32_t CompressMethodTest::VintCompress(uint32_t* posBuffer, 
        uint32_t len) {

    int32_t totalLen = 0;
    uint8_t *cursor = (uint8_t*)(_dataBuffer);
    for (uint32_t i = 0; i < len; i++) {

        int32_t num = VByteCompressor::EncodeVInt32(
                cursor, _bufferSize, posBuffer[i]);
        cursor = cursor + num;
        totalLen += num;
    }
    
    return totalLen;
}

int32_t CompressMethodTest::P4DeltaCompress(uint32_t* posBuffer, 
        uint32_t len) {
    
    /* position list format
       ||skipLen||posLen||skipPos(i)||skipOffset(i)||posBlock{i)
    */
    uint8_t *cursor =(uint8_t*)_dataBuffer;
    NewPForDeltaCompressor compressor;
    uint32_t compressSize = compressor.CompressInt32((uint32_t *)cursor,
            _bufferSize, posBuffer, len);
    return sizeof(int32_t) * compressSize;
}


void CompressMethodTest::Compress(uint32_t* posBuffer, uint32_t len) 
{
  string ctDesc = "none";
  int32_t inputLen = len * sizeof(uint32_t);
  int32_t totalLen = inputLen;
  uint8_t compressMethod = CT_NONE;

  if (!_dataBuffer)
  {
    _dataBuffer = new char[_bufferSize];
    assert(_dataBuffer);
  }

  if (len > 5)
  {      
      int32_t gvint = GroupVarint::Compress((uint8_t*)_dataBuffer, 
              _bufferSize, posBuffer, len);

      int32_t p4dLen = P4DeltaCompress(posBuffer, len);
      if (gvint <= p4dLen)
      {
	  totalLen = gvint;
	  ctDesc = "gvint";
          compressMethod = CT_VINT;
      }
      else
      {
	  totalLen = p4dLen;
	  ctDesc = "p4delta";
          compressMethod = CT_P4DELTA;
      }
    
      if (totalLen > inputLen)
      {
          totalLen = inputLen;
          ctDesc = "none";
          _shouldNotComCount += len;
 	  _shouldNotComCSize += totalLen;
 	  _shouleNotTermCount++;
      }

      if (compressMethod == CT_NONE)
      {    
          _noComCount += len;
          _noComSize += len * sizeof(uint32_t);
          _noCompressTermCount++;
      }
      else if (compressMethod == CT_P4DELTA)
      {
	  _p4deltaComCount += len;
	  _p4deltaComSize += p4dLen;
	  _p4deltaTermCount++;
	  _p4deltaGainSize += gvint - p4dLen;
      }
      else 
      {
          _vintComCount += len;
	  _vintComSize += gvint;
	  _vintTermCount++;
      }
  }
  else
  {
      _noComCount += len;
      _noComSize += len * sizeof(uint32_t);
      _noCompressTermCount++;
  }

    _lenBeforeCompress += inputLen;
    _lenAfterCompress += totalLen;
    ++_countCallTimes;
}

IE_NAMESPACE_END(index);

