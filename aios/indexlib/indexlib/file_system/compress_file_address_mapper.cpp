#include "indexlib/file_system/compress_file_address_mapper.h"
#include "indexlib/file_system/block_file_node.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/file_system/slice_file_writer.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/path_util.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, CompressFileAddressMapper);

CompressFileAddressMapper::CompressFileAddressMapper()
    : mBaseAddr(NULL)
    , mPowerOf2(0)
    , mPowerMark(0)
    , mBlockCount(0)
    , mMaxCompressBlockSize(0)
{}

CompressFileAddressMapper::~CompressFileAddressMapper() 
{}

void CompressFileAddressMapper::InitForRead(const CompressFileInfo& fileInfo,
        const FileReaderPtr& reader, Directory* directory)
{
    assert(reader);
    mBlockCount = fileInfo.blockCount;
    InitBlockMask(fileInfo.blockSize);
    size_t mapperDataLength = sizeof(size_t) * (mBlockCount + 1);
    if (reader->GetLength() != (fileInfo.compressFileLen + mapperDataLength))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "compress file [%s] is collapsed,"
                             " compressLen [%lu], mapperLen [%lu], fileLen [%lu]!",
                             reader->GetPath().c_str(), fileInfo.compressFileLen,
                             mapperDataLength, reader->GetLength());
    }
    
    char* baseAddr = (char*)reader->GetBaseAddress();
    if (baseAddr)
    {
        mBaseAddr = (size_t*)(baseAddr + fileInfo.compressFileLen);
    }
    else
    {
        std::string compressFileMapperPath = GetBlockOffsetMapperFileName(reader, directory);
        size_t mapperFileLen = sizeof(size_t) * (mBlockCount + 1);
        mBlockOffsetSliceFile =
            directory->CreateSliceFile(compressFileMapperPath, mapperFileLen, 1);

        SliceFileReaderPtr sliceReader = mBlockOffsetSliceFile->CreateSliceFileReader();
        if (sliceReader->GetLength() == 0)
        {
            vector<char> tmpBuffer;
            tmpBuffer.resize(mapperFileLen);
            if (mapperFileLen != reader->Read(
                            (char*)tmpBuffer.data(), mapperFileLen, fileInfo.compressFileLen))
            {
                INDEXLIB_FATAL_ERROR(FileIO, "read address mapper data failed from file[%s]",
                        reader->GetPath().c_str());
            }
            SliceFileWriterPtr sliceWriter = mBlockOffsetSliceFile->CreateSliceFileWriter();
            sliceWriter->Write(tmpBuffer.data(), mapperFileLen);
            sliceWriter->Close();
        }
        if (sliceReader->GetLength() != mapperFileLen)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "address mapper file [%s] get wrong file length [%lu / %lu]",
                    sliceReader->GetPath().c_str(), sliceReader->GetLength(), mapperFileLen);
        }
        mBaseAddr = (size_t*)sliceReader->GetBaseAddress();
    }
}

void CompressFileAddressMapper::InitForWrite(size_t blockSize)
{
    InitBlockMask(blockSize);
    assert(mBlockOffsetVec.empty());
    mBlockOffsetVec.push_back(0);
    mBaseAddr = (size_t*)mBlockOffsetVec.data();
    mBlockCount = 0;
}

void CompressFileAddressMapper::AddOneBlock(size_t compBlockSize)
{
    if (mBlockOffsetVec.empty())
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "not support add block info!");
    }
    mBlockOffsetVec.push_back(*mBlockOffsetVec.rbegin() + compBlockSize);
    mBaseAddr = (size_t*)mBlockOffsetVec.data();
    ++mBlockCount;
}

void CompressFileAddressMapper::InitBlockMask(size_t blockSize)
{
    size_t alignedSize = 1;
    mPowerOf2 = 0;
    while (alignedSize < blockSize)
    {
        alignedSize <<= 1;
        mPowerOf2++;
    }
    mPowerMark = alignedSize - 1;
    if (alignedSize != blockSize)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "blockSize [%lu] must be 2^n!", blockSize);
    }
}

void CompressFileAddressMapper::Dump(const file_system::FileWriterPtr& writer)
{
    assert(writer);
    writer->Write(Data(), Size());
}

size_t CompressFileAddressMapper::GetMaxCompressBlockSize()
{
    if (mMaxCompressBlockSize == 0)
    {
        for (size_t i = 0; i < mBlockCount; i++)
        {
            mMaxCompressBlockSize = max(mMaxCompressBlockSize, CompressBlockLength(i));
        }
    }
    return mMaxCompressBlockSize;
}

string CompressFileAddressMapper::GetBlockOffsetMapperFileName(
            const FileReaderPtr& reader, const Directory* directory)
{
    string compressFileRelativePath;
    if (!PathUtil::GetRelativePath(directory->GetPath(), reader->GetPath(),
                                   compressFileRelativePath))
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "make compressFileRelativePath failed,"
                             " directory path [%s], file path [%s] ",
                             directory->GetPath().c_str(),
                             reader->GetPath().c_str());
    }
    return compressFileRelativePath + ".address_mapper";
}

IE_NAMESPACE_END(file_system);

