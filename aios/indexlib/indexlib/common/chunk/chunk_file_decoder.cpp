#include "indexlib/common/chunk/chunk_file_decoder.h"
#include "indexlib/common/chunk/chunk_decoder_creator.h"
#include "indexlib/file_system/compress_file_reader.h"


using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ChunkFileDecoder);

ChunkFileDecoder::ChunkFileDecoder()
    : mPool(NULL)
    , mReaderBaseAddr(NULL)
    , mOpenType(FSOT_UNKNOWN)
    , mCurrentChunkOffset(numeric_limits<uint64_t>::max())
    , mRecordLen(0)
    , mChunkDecoder(NULL)
    , mLoadChunkCount(0)
    , mReadSize(0)
{
}

ChunkFileDecoder::~ChunkFileDecoder() 
{
    ReleaseChunkDecoder();
}

void ChunkFileDecoder::Init(Pool* sessionPool, FileReader* fileReader, uint32_t recordLen)
{
    mPool = sessionPool;
    mFileReader = fileReader;
    mReaderBaseAddr = (const char*)fileReader->GetBaseAddress();
    mOpenType = fileReader->GetOpenType();
    mCurrentChunkOffset = numeric_limits<uint64_t>::max();
    mChunkDecoder = NULL;
    mRecordLen = recordLen;
}

void ChunkFileDecoder::Prefetch(uint64_t size, uint64_t offset, ReadOption option)
{
    if (mReaderBaseAddr)
    {
        return;
    }
    mFileReader->Prefetch(size, offset, option);
}

#define CALL_FUNCTION_MACRO(funcName, args...)                          \
    switch (mChunkDecoder->GetType())                                   \
    {                                                                   \
    case ct_plain_integrated:                                           \
        funcName<IntegratedPlainChunkDecoder>(args);                    \
        break;                                                          \
    case ct_plain_slice:                                                \
    case ct_encoded:                                                    \
    default:                                                            \
        assert(false);                                                  \
    }

#define CALL_TEMPLATE_FUNCTION_MACRO(type, funcName, args...)           \
    switch (mChunkDecoder->GetType())                                   \
    {                                                                   \
    case ct_plain_integrated:                                           \
        funcName<IntegratedPlainChunkDecoder, type>(args);              \
        break;                                                          \
    case ct_plain_slice:                                                \
    case ct_encoded:                                                    \
    default:                                                            \
        assert(false);                                                  \
    }

void ChunkFileDecoder::Seek(uint64_t chunkOffset, uint32_t inChunkOffset, file_system::ReadOption option)
{
    if (mCurrentChunkOffset != chunkOffset || !mChunkDecoder)
    {
        SwitchChunkDecoder(chunkOffset, option);
    }
    CALL_FUNCTION_MACRO(DoSeekInChunk, inChunkOffset);
}

void ChunkFileDecoder::ReadRecord(autil::ConstString& value, ReadOption option)
{
    if (!mChunkDecoder)
    {
        LoadChunkDecoder(mCurrentChunkOffset, option);
    }
    CALL_FUNCTION_MACRO(DoReadRecord, value);
}

void ChunkFileDecoder::ReadRecord(autil::ConstString& value,
                                  uint64_t chunkOffset,
                                  uint32_t inChunkOffset,
                                  ReadOption option)
{
    if (mCurrentChunkOffset != chunkOffset || !mChunkDecoder)
    {
        SwitchChunkDecoder(chunkOffset, option);
    }
    CALL_FUNCTION_MACRO(DoReadRecord, value, inChunkOffset);
}

void ChunkFileDecoder::Read(autil::ConstString& value, uint32_t len, ReadOption option)
{
    if (!mChunkDecoder)
    {
        LoadChunkDecoder(mCurrentChunkOffset, option);
    }
    CALL_FUNCTION_MACRO(DoRead, value, len);
}

#undef CALL_FUNCTION_MACRO

template<typename DecoderType>
void ChunkFileDecoder::DoSeekInChunk(uint32_t inChunkOffset)
{
    DecoderType* chunkDecoder = static_cast<DecoderType*>(mChunkDecoder);
    chunkDecoder->Seek(inChunkOffset);
}

template<typename DecoderType>
void ChunkFileDecoder::DoRead(autil::ConstString& value, uint32_t len)
{
    DecoderType* chunkDecoder = static_cast<DecoderType*>(mChunkDecoder);
    mReadSize += chunkDecoder->Read(value, len);
    if (chunkDecoder->IsEnd())
    {
        mCurrentChunkOffset += mChunkDecoder->GetChunkLength() + sizeof(ChunkMeta);
        ReleaseChunkDecoder();
    }
}

template<typename DecoderType>
void ChunkFileDecoder::DoReadRecord(autil::ConstString& value)
{
    DecoderType* chunkDecoder = static_cast<DecoderType*>(mChunkDecoder);
    mReadSize += chunkDecoder->ReadRecord(value);
    if (chunkDecoder->IsEnd())
    {
        mCurrentChunkOffset += mChunkDecoder->GetChunkLength() + sizeof(ChunkMeta);
        ReleaseChunkDecoder();
    }
}

template<typename DecoderType>
void ChunkFileDecoder::DoReadRecord(autil::ConstString& value, uint32_t inChunkOffset)
{
    DoSeekInChunk<DecoderType>(inChunkOffset);
    DoReadRecord<DecoderType>(value);
}

void ChunkFileDecoder::LoadChunkDecoder(uint64_t chunkOffset, ReadOption option)
{
    if (mReaderBaseAddr)
    {
        mChunkDecoder = common::ChunkDecoderCreator::Create(
            mReaderBaseAddr, chunkOffset, mRecordLen, mPool);
    }
    else
    {
        mChunkDecoder = common::ChunkDecoderCreator::Create(
            mFileReader, mOpenType, chunkOffset, mRecordLen, mPool, option);
    }
    ++mLoadChunkCount;
    mCurrentChunkOffset = chunkOffset;
}

void ChunkFileDecoder::SwitchChunkDecoder(uint64_t chunkOffset, ReadOption option)
{
    ReleaseChunkDecoder();
    LoadChunkDecoder(chunkOffset, option);
}
void ChunkFileDecoder::ReleaseChunkDecoder()
{
    if (mChunkDecoder)
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mChunkDecoder);
        mChunkDecoder = NULL;
    }
}

IE_NAMESPACE_END(common);

