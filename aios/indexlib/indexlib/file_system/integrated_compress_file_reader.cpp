#include "indexlib/file_system/integrated_compress_file_reader.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, IntegratedCompressFileReader);

bool IntegratedCompressFileReader::Init(const FileReaderPtr& fileReader,
                                        const CompressFileInfo& compressInfo, Directory* directory)
{
    if (!CompressFileReader::Init(fileReader, compressInfo, directory))
    {
        return false;
    }
    
    mDataAddr = (char*)fileReader->GetBaseAddress();
    return mDataAddr != NULL;
}

IntegratedCompressFileReader* IntegratedCompressFileReader::CreateSessionReader(Pool* pool)
{
    assert(mDataFileReader);
    assert(mCompressAddrMapper);
    IntegratedCompressFileReader* cloneReader = IE_POOL_COMPATIBLE_NEW_CLASS(
            pool, IntegratedCompressFileReader, pool);
    cloneReader->DoInit(mDataFileReader, mCompressAddrMapper, mCompressInfo);
    cloneReader->mDataAddr = mDataAddr;
    return cloneReader;
}

void IntegratedCompressFileReader::LoadBuffer(size_t offset, ReadOption option)
{
    assert(!InCurrentBlock(offset));
    assert(mDataFileReader);
    size_t blockIdx = mCompressAddrMapper->OffsetToBlockIdx(offset);
    size_t compressBlockOffset = mCompressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = mCompressAddrMapper->CompressBlockLength(blockIdx);
    mCompressor->Reset();
    if (!mCompressor->DecompressToOutBuffer(mDataAddr + compressBlockOffset,
                    compressBlockSize, mCompressInfo.blockSize))
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

