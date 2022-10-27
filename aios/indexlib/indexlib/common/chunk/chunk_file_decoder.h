#ifndef __INDEXLIB_CHUNK_FILE_DECODER_H
#define __INDEXLIB_CHUNK_FILE_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/common/chunk/chunk_decoder.h"

IE_NAMESPACE_BEGIN(common);

class ChunkFileDecoder
{
public:
    ChunkFileDecoder();
    ~ChunkFileDecoder();
public:
    void Init(autil::mem_pool::Pool* sessionPool,
              file_system::FileReader* fileReader,
              uint32_t recordLen);

    void Seek(uint64_t chunkOffset, uint32_t inChunkOffset, file_system::ReadOption option);
    void Prefetch(uint64_t size, uint64_t offset, file_system::ReadOption option);

    // shuold not cross chunk
    void Read(autil::ConstString& value, uint32_t len, file_system::ReadOption option);

    void ReadRecord(autil::ConstString& value, file_system::ReadOption option);
    void ReadRecord(autil::ConstString& value, uint64_t chunkOffset, uint32_t inChunkOffset,
        file_system::ReadOption option);

    int64_t GetLoadChunkCount() const
    {
        return mLoadChunkCount;
    }

    int64_t GetReadSize() const
    {
        return mReadSize;
    }

    bool IsIntegratedFile() const { return mReaderBaseAddr != nullptr; }

private:
    template<typename DecoderType>
    void DoRead(autil::ConstString& value, uint32_t len);

    template<typename DecoderType>
    void DoReadRecord(autil::ConstString& value);

    template<typename DecoderType>
    void DoReadRecord(autil::ConstString& value, uint32_t inChunkOffset);

    template<typename DecoderType>
    void DoSeekInChunk(uint32_t inChunkOffset);

    void ReleaseChunkDecoder();
    void LoadChunkDecoder(uint64_t chunkOffset, file_system::ReadOption option);
    void SwitchChunkDecoder(uint64_t chunkOffset, file_system::ReadOption option);
    
private:
    autil::mem_pool::Pool* mPool;
    const char* mReaderBaseAddr;
    file_system::FileReader* mFileReader;
    file_system::FSOpenType mOpenType;
    uint64_t mCurrentChunkOffset;
    uint32_t mRecordLen;
    ChunkDecoder* mChunkDecoder;
    int64_t mLoadChunkCount;
    int64_t mReadSize;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ChunkFileDecoder);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CHUNK_FILE_DECODER_H
