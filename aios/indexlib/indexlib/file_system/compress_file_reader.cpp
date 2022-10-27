#include "indexlib/file_system/compress_file_reader.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, CompressFileReader);

bool CompressFileReader::Init(const FileReaderPtr& fileReader,
                              const CompressFileInfo& compressInfo, Directory* directory)
{
    CompressFileAddressMapperPtr mapper =
        LoadCompressAddressMapper(fileReader, compressInfo, directory);
    if (!mapper)
    {
        return false;
    }
    return DoInit(fileReader, mapper, compressInfo);
}

CompressFileAddressMapperPtr CompressFileReader::LoadCompressAddressMapper(
        const FileReaderPtr& fileReader,
        const CompressFileInfo& compressInfo, Directory* directory)
{
    if (!fileReader)
    {
        return CompressFileAddressMapperPtr();
    }
    CompressFileAddressMapperPtr mapper(new CompressFileAddressMapper);
    mapper->InitForRead(compressInfo, fileReader, directory);
    return mapper;
}

size_t CompressFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option)
{
    assert(mCompressAddrMapper);
    if (offset >= GetUncompressedFileLength())
    {
        return 0;
    }
    mOffset = offset;
    return Read(buffer, length, option);
}

void CompressFileReader::PrefetchData(size_t length, size_t offset, ReadOption option)
{
    size_t blockSize = mCompressAddrMapper->GetBlockSize();
    size_t inBlockOffset = mCompressAddrMapper->OffsetToInBlockOffset(offset);
    if (InCurrentBlock(offset))
    {
        if (length <= blockSize - inBlockOffset)
        {
            return;
        }
        offset += (blockSize - inBlockOffset);
        length -= (blockSize - inBlockOffset);
    }
    if (offset >= mDataFileReader->GetLength())
    {
        return;
    }
    // [begin, end)
    size_t beginIdx = mCompressAddrMapper->OffsetToBlockIdx(offset);
    size_t endIdx = mCompressAddrMapper->OffsetToBlockIdx(offset + length + blockSize - 1);
    size_t beginOffset = mCompressAddrMapper->CompressBlockAddress(beginIdx);
    size_t endOffset = mDataFileReader->GetLength();
    if (endIdx < mCompressAddrMapper->GetBlockCount())
    {
        endOffset = mCompressAddrMapper->CompressBlockAddress(endIdx);
    }
    mDataFileReader->Prefetch(endOffset - beginOffset, beginOffset, option);
}

size_t CompressFileReader::Read(void* buffer, size_t length, ReadOption option)
{
    PrefetchData(length, mOffset, option);

    int64_t leftLen = length;
    char* cursor = (char*)buffer;
    while (true)
    {
        if (!InCurrentBlock(mOffset))
        {
            LoadBuffer(mOffset, option);
        }
        size_t inBlockOffset = mCompressAddrMapper->OffsetToInBlockOffset(mOffset);
        int64_t dataLeftInBlock = mCompressor->GetBufferOutLen() - inBlockOffset;
        int64_t lengthToCopy = leftLen < dataLeftInBlock ? leftLen : dataLeftInBlock;

        memcpy(cursor, mCompressor->GetBufferOut() + inBlockOffset, lengthToCopy);
        cursor += lengthToCopy;
        leftLen -= lengthToCopy;
        mOffset += lengthToCopy;

        if (leftLen <= 0)
        {
            assert(leftLen == 0);
            return length;
        }

        if (mOffset >= GetUncompressedFileLength())
        {
            return (cursor - (char*)buffer);
        }
    }
    return 0;
}

bool CompressFileReader::DoInit(const FileReaderPtr& fileReader,
                                const CompressFileAddressMapperPtr& compressAddressMapper,
                                const CompressFileInfo& compressInfo)
{
    assert(fileReader);
    assert(compressAddressMapper);
    mDataFileReader = fileReader;
    mCompressAddrMapper = compressAddressMapper;
    mCompressInfo = compressInfo;

    mCompressor = CreateCompressor(compressAddressMapper, compressInfo, mPool);
    mCurBlockIdx = mCompressAddrMapper->GetBlockCount();
    mOffset = 0;
    return true;
}

BufferCompressor* CompressFileReader::CreateCompressor(
        const CompressFileAddressMapperPtr& compressAddressMapper,
        const CompressFileInfo& compressInfo, Pool* pool)
{
    BufferCompressor* compressor = NULL;
    if (pool)
    {
        compressor = BufferCompressorCreator::CreateBufferCompressor(
                pool, compressInfo.compressorName, compressInfo.blockSize);
        if (!compressor)
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "create buffer compressor [%s] failed!",
                    compressInfo.compressorName.c_str());
        }
    }
    else
    {
        compressor = BufferCompressorCreator::CreateBufferCompressor(
                compressInfo.compressorName);
        if (!compressor)
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "create buffer compressor [%s] failed!",
                    compressInfo.compressorName.c_str());
        }
        compressor->SetBufferInLen(compressAddressMapper->GetMaxCompressBlockSize());
        compressor->SetBufferOutLen(compressInfo.blockSize);
    }
    return compressor;
}

IE_NAMESPACE_END(file_system);

