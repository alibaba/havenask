#include "indexlib/file_system/snappy_compress_file_reader.h"
#include "indexlib/file_system/compress_file_meta.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SnappyCompressFileReader);

bool SnappyCompressFileReader::Init(const FileReaderPtr& fileReader)
{
    CompressFileMetaPtr compressMeta = LoadCompressMeta(fileReader);
    if (!compressMeta)
    {
        return false;
    }
    
    assert(fileReader);
    mDataFileReader = fileReader;
    mCompressMeta = compressMeta;
    mCompressor.SetBufferInLen(mCompressMeta->GetMaxCompressBlockSize());
    mCompressor.SetBufferOutLen(mBlockSize);

    mCurInBlockOffset = 0;
    mOriFileLength = mCompressMeta->GetUnCompressFileLength();
    mCurBlockBeginOffset = mOriFileLength;
    mCurBlockIdx = mCompressMeta->GetBlockCount();
    mOffset = 0;
    return true;
}

size_t SnappyCompressFileReader::GetUncompressedFileLength() const
{
    assert(mCompressMeta);
    return mCompressMeta->GetUnCompressFileLength();
}

CompressFileMetaPtr SnappyCompressFileReader::LoadCompressMeta(
        const FileReaderPtr& fileReader)
{
    if (!fileReader)
    {
        return CompressFileMetaPtr();
    }
    
    size_t fileLen = fileReader->GetLength();
    if (fileLen == 0)
    {
        return CompressFileMetaPtr(new CompressFileMeta());
    }
    
    size_t blockCount;
    if (fileLen < (sizeof(blockCount) + sizeof(mBlockSize)))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Invalid compress file[%s]", 
                             fileReader->GetPath().c_str());
        return CompressFileMetaPtr();
    }
    int64_t readOffset = fileLen - sizeof(blockCount);
    fileReader->Read(&blockCount, sizeof(blockCount), readOffset);
    readOffset -= sizeof(mBlockSize);
    fileReader->Read(&mBlockSize, sizeof(mBlockSize), readOffset);
    size_t metaLen = CompressFileMeta::GetMetaLength(blockCount);
    readOffset -= metaLen;
    if (readOffset < 0)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Invalid compress file[%s]", 
                             fileReader->GetPath().c_str());
        return CompressFileMetaPtr();
    }

    vector<char> tmpBuffer(metaLen);
    if (metaLen != fileReader->Read(tmpBuffer.data(), metaLen, readOffset))
    {
        return CompressFileMetaPtr();
    }

    CompressFileMetaPtr meta(new CompressFileMeta());
    if (!meta->Init(blockCount, tmpBuffer.data()))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Invalid compress file[%s]", 
                             fileReader->GetPath().c_str());
        return CompressFileMetaPtr();
    }
    return meta;
}

bool SnappyCompressFileReader::InCurrentBlock(size_t offset) const
{
    return offset >= mCurBlockBeginOffset && 
        offset < mCurBlockBeginOffset + mCompressor.GetBufferOutLen();
}

bool SnappyCompressFileReader::LocateBlockOffsetInCompressFile(
        size_t offset, size_t &blockIdx, size_t &uncompBlockOffset, 
        size_t &compBlockOffset, size_t &compBlockSize) const
{
    assert(mCompressMeta);
    const vector<CompressFileMeta::CompressBlockMeta>& blockMetas = 
        mCompressMeta->GetCompressBlockMetas();

    size_t beginIdx = 0;
    size_t endIdx = 0;
    if (offset >= mCurBlockBeginOffset)
    {
        beginIdx = mCurBlockIdx;
        endIdx = blockMetas.size();
    }
    else
    {
        beginIdx = 0;
        endIdx = mCurBlockIdx;
    }

    for (size_t i = beginIdx; i < endIdx; ++i)
    {
        if (offset < blockMetas[i].uncompressEndPos)
        {
            blockIdx = i;
            uncompBlockOffset = (i == 0) ? 0 : blockMetas[i-1].uncompressEndPos;
            compBlockOffset = (i == 0) ? 0 : blockMetas[i-1].compressEndPos;
            compBlockSize = blockMetas[i].compressEndPos - compBlockOffset;
            return true;
        }
    }
    return false;
}

void SnappyCompressFileReader::LoadBuffer(size_t offset, ReadOption option)
{
    assert(mDataFileReader);
    size_t uncompressBlockOffset = 0;
    size_t compressBlockOffset = 0;
    size_t compressBlockSize = 0;
    size_t curBlockIdx = 0;
    if (!LocateBlockOffsetInCompressFile(
                    offset, curBlockIdx, uncompressBlockOffset, 
                    compressBlockOffset, compressBlockSize))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "locate block offset [%lu] fail!", offset);
        return;
    }

    mCompressor.Reset();
    DynamicBuf& inBuffer = mCompressor.GetInBuffer();
    if (compressBlockSize != mDataFileReader->Read(
            inBuffer.getBuffer(), compressBlockSize, compressBlockOffset, option))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file[%s] failed", 
                             mDataFileReader->GetPath().c_str());
        return;
    }
    
    inBuffer.movePtr(compressBlockSize);
    if (!mCompressor.Decompress(mBlockSize))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file [%s] failed", 
                             mDataFileReader->GetPath().c_str());
        return;
    }
    mCurBlockIdx = curBlockIdx;
    mCurBlockBeginOffset = uncompressBlockOffset;
    mCurInBlockOffset = offset - mCurBlockBeginOffset;
}

size_t SnappyCompressFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    assert(mCompressMeta);
    if (offset >= mOriFileLength)
    {
        return 0;
    }
    mOffset = offset;
    return Read(buffer, length, option);
}

size_t SnappyCompressFileReader::Read(void* buffer, size_t length, ReadOption option)
{
    int64_t leftLen = length;
    char* cursor = (char*)buffer;
    while (true)
    {
        if (!InCurrentBlock(mOffset))
        {
            LoadBuffer(mOffset, option);
        }
        else
        {
            mCurInBlockOffset = mOffset - mCurBlockBeginOffset;
        }

        int64_t dataLeftInBlock = mCompressor.GetBufferOutLen() - mCurInBlockOffset;
        int64_t lengthToCopy = leftLen < dataLeftInBlock ? leftLen : dataLeftInBlock;

        memcpy(cursor, mCompressor.GetBufferOut() + mCurInBlockOffset, lengthToCopy);
        cursor += lengthToCopy;
        leftLen -= lengthToCopy;
        mOffset += lengthToCopy;
        mCurInBlockOffset += lengthToCopy;

        if (leftLen <= 0)
        {
            assert(leftLen == 0);
            return length;
        }

        if (mOffset >= mOriFileLength)
        {
            return (cursor - (char*)buffer);
        }
    }
    return 0;

}

IE_NAMESPACE_END(file_system);

