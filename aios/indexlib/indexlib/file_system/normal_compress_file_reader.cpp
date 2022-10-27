#include "indexlib/file_system/normal_compress_file_reader.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, NormalCompressFileReader);

NormalCompressFileReader* NormalCompressFileReader::CreateSessionReader(Pool* pool) 
{
    assert(mDataFileReader);
    assert(mCompressAddrMapper);
    NormalCompressFileReader* cloneReader = IE_POOL_COMPATIBLE_NEW_CLASS(
            pool, NormalCompressFileReader, pool);
    cloneReader->DoInit(mDataFileReader, mCompressAddrMapper, mCompressInfo);
    return cloneReader;
}

void NormalCompressFileReader::LoadBuffer(size_t offset, ReadOption option)
{
    assert(!InCurrentBlock(offset));
    assert(mDataFileReader);
    size_t blockIdx = mCompressAddrMapper->OffsetToBlockIdx(offset);
    size_t compressBlockOffset = mCompressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = mCompressAddrMapper->CompressBlockLength(blockIdx);
    mCompressor->Reset();

    assert(mDataFileReader);
    DynamicBuf& inBuffer = mCompressor->GetInBuffer();
    if (compressBlockSize != mDataFileReader->Read(
            inBuffer.getBuffer(), compressBlockSize, compressBlockOffset, option))
    {
        INDEXLIB_FATAL_ERROR(FileIO,
                             "read compress file[%s] failed, offset [%lu], compress len [%lu]", 
                             mDataFileReader->GetPath().c_str(),
                             compressBlockOffset, compressBlockSize);
        return;
    }
    inBuffer.movePtr(compressBlockSize);
    if (!mCompressor->Decompress(mCompressInfo.blockSize))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "decompress file [%s] failed, offset [%lu], compress len [%lu]", 
                             mDataFileReader->GetPath().c_str(),
                             compressBlockOffset, compressBlockSize);
        return;
    }
    mCurBlockIdx = blockIdx;
}

IE_NAMESPACE_END(file_system);

