#include "indexlib/index/normal/attribute/accessor/compress_offset_reader.h"
#include "indexlib/index_define.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, CompressOffsetReader);

CompressOffsetReader::CompressOffsetReader() 
    : mU32CompressReader(NULL)
    , mU64CompressReader(NULL)
{
}

CompressOffsetReader::~CompressOffsetReader() 
{
    DELETE_AND_SET_NULL(mU32CompressReader);
    DELETE_AND_SET_NULL(mU64CompressReader);
}

void CompressOffsetReader::Init(
        uint8_t* buffer, uint64_t bufferLen, uint32_t docCount,
        SliceFileReaderPtr expandSliceFile)
{
    assert(bufferLen >= 4);
    assert(!mU32CompressReader);
    assert(!mU64CompressReader);
    size_t compressLen = bufferLen - sizeof(uint32_t); // minus tail len
    uint32_t magicTail = *(uint32_t*)(buffer + bufferLen - 4);
    uint32_t itemCount = 0;

    if (magicTail == UINT32_OFFSET_TAIL_MAGIC)
    {
        if (expandSliceFile)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "updatable compress offset should be u64 offset" );
        }

        mU32CompressReader = new EquivalentCompressReader<uint32_t>();
        mU32CompressReader->Init(buffer, compressLen);
        itemCount = mU32CompressReader->Size();
    }
    else if (magicTail == UINT64_OFFSET_TAIL_MAGIC)
    {
        BytesAlignedSliceArrayPtr byteSliceArray;
        if (expandSliceFile)
        {
            byteSliceArray = expandSliceFile->GetBytesAlignedSliceArray();
        }

        mU64CompressReader = new EquivalentCompressReader<uint64_t>();
        mU64CompressReader->Init(buffer, compressLen, byteSliceArray);
        itemCount = mU64CompressReader->Size();
    } 
    else
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Attribute compressed offset file magic tail not match" );
    }

    //docCount + 1: add the last guard offset
    if (docCount + 1 != itemCount)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "Attribute compressed offset item size do not match docCount" );
    }
}

IE_NAMESPACE_END(index);

