#include "indexlib/index/normal/attribute/accessor/uncompress_offset_reader.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, UncompressOffsetReader);

UncompressOffsetReader::UncompressOffsetReader()
    : mU64Buffer(NULL)
    , mU32Buffer(NULL)
    , mDocCount(0)
{
}

UncompressOffsetReader::~UncompressOffsetReader() 
{
}

void UncompressOffsetReader::Init(
        uint8_t* buffer, uint64_t bufferLen, uint32_t docCount,
        SliceFileReaderPtr u64SliceFile)
{
    if (bufferLen == (docCount + 1) * sizeof(uint32_t)) 
    {
        mU32Buffer = (uint32_t*)buffer;
    }
    else if (bufferLen == (docCount + 1) * sizeof(uint64_t)) 
    {
        mU64Buffer = (uint64_t*)buffer;
    }
    else {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Attribute offset file length is not consistent "
                             "with segment info, offset length is %ld, "
                             "doc number in segment info is %d", 
                             bufferLen, (int32_t)docCount);
    }
    mDocCount = docCount;
}

void UncompressOffsetReader::ExtendU32OffsetToU64Offset(
        const SliceFileReaderPtr& u64SliceFileReader)
{
    assert(u64SliceFileReader);
    BytesAlignedSliceArrayPtr sliceArray = 
        u64SliceFileReader->GetBytesAlignedSliceArray();
    
    uint32_t bufferItemCount = 64 * 1024;

    // add extend buffer to decrease call Insert interface
    uint64_t *extendOffsetBuffer = new uint64_t[bufferItemCount];

    uint32_t remainCount = mDocCount + 1;
    uint32_t *u32Begin = mU32Buffer;
    while(remainCount > 0)
    {
        uint32_t convertNum = bufferItemCount <= remainCount ? 
                              bufferItemCount : remainCount;
        ExtendU32OffsetToU64Offset(u32Begin, extendOffsetBuffer, convertNum);
        sliceArray->Insert(extendOffsetBuffer, 
                convertNum * sizeof(uint64_t));
        remainCount -= convertNum;
        u32Begin += convertNum;
    }

    delete[] extendOffsetBuffer;
    mU64Buffer = (uint64_t*)u64SliceFileReader->GetBaseAddress();
}

void UncompressOffsetReader::ExtendU32OffsetToU64Offset(
        uint32_t* u32Offset, uint64_t* u64Offset, uint32_t count)
{
    const uint32_t pipeLineNum = 8;
    uint32_t processMod = count % pipeLineNum;
    uint32_t processSize = count - processMod;
    uint32_t pos = 0;
    for (; pos < processSize; pos += pipeLineNum)
    {
        u64Offset[pos] = u32Offset[pos];
        u64Offset[pos+1] = u32Offset[pos+1];
        u64Offset[pos+2] = u32Offset[pos+2];
        u64Offset[pos+3] = u32Offset[pos+3];
        u64Offset[pos+4] = u32Offset[pos+4];
        u64Offset[pos+5] = u32Offset[pos+5];
        u64Offset[pos+6] = u32Offset[pos+6];
        u64Offset[pos+7] = u32Offset[pos+7];
    }
    for (; pos < count; ++pos)
    {
        u64Offset[pos] = u32Offset[pos];
    }
}
        
IE_NAMESPACE_END(index);

