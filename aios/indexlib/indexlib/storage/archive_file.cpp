#include "indexlib/storage/archive_file.h"
#include "autil/StringUtil.h"

using namespace std;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, ArchiveFile);

ArchiveFile::~ArchiveFile() 
{
    delete mBuffer;
}

size_t ArchiveFile::Read(void* buffer, size_t length) 
{
    size_t readLength = 0;
    vector<ReadBlockInfo> readBlockInfos;
    CalculateBlockInfo(mOffset, length, readBlockInfos);
    char* cursor = (char*)buffer;
    for (size_t i = 0; i < readBlockInfos.size(); i++)
    {
        FillBufferFromBlock(readBlockInfos[i], cursor);
        readLength += readBlockInfos[i].readLength;
        cursor += readBlockInfos[i].readLength;
        mOffset += readBlockInfos[i].readLength;
     }
    return readLength;
}

void ArchiveFile::FillBufferFromBlock(const ReadBlockInfo& readInfo, char* buffer)
{
    if (mCurrentBlock != readInfo.blockIdx)
    {
        FillReadBuffer(readInfo.blockIdx);
        mCurrentBlock = readInfo.blockIdx;
    }
    memcpy(buffer, mBuffer->GetBaseAddr() + readInfo.beginPos, readInfo.readLength);
}

void ArchiveFile::FillReadBuffer(int64_t blockIdx)
{
    string blockKey = mFileMeta.blocks[blockIdx].key;
    int64_t length = mFileMeta.blocks[blockIdx].length;
    mBuffer->SetCursor(0);
    mLogFile->Read(blockKey, mBuffer->GetBaseAddr(), length); 
}

void ArchiveFile::CalculateBlockInfo(int64_t offset, size_t length,
                                    vector<ReadBlockInfo>& readInfos)
{
    int64_t blockBeginOffset = 0;
    int64_t blockEndOffset = 0;
    for (int64_t i = 0; i < (int64_t)mFileMeta.blocks.size(); i++)
    {
        blockEndOffset = blockBeginOffset + mFileMeta.blocks[i].length;
        if (blockBeginOffset >= offset + (int64_t)length)
        {
            break;
        }

        if (blockEndOffset <= offset)
        {
            blockBeginOffset += mFileMeta.blocks[i].length;
            continue;
        }
        
        ReadBlockInfo readBlock;
        readBlock.blockIdx = i;
        readBlock.beginPos = 
            offset >= blockBeginOffset ? offset - blockBeginOffset : 0;
        readBlock.readLength = 
            offset + (int64_t)length >= blockEndOffset ?
            mFileMeta.blocks[i].length - readBlock.beginPos :
            mFileMeta.blocks[i].length - readBlock.beginPos - 
            (blockEndOffset - offset - length);
        readInfos.push_back(readBlock);
        blockBeginOffset += mFileMeta.blocks[i].length;
    }
}

size_t ArchiveFile::PRead(void* buffer, size_t length, off_t offset) 
{
    Seek(offset, fslib::FILE_SEEK_SET);
    return Read(buffer, length);
}

void ArchiveFile::Open(const std::string& fileName, 
                       const LogFilePtr& logFile) 
{
    mFileName = fileName;
    mLogFile = logFile;
    if (!mIsReadOnly)
    {
        return;
    }
    string metaKey = GetArchiveFileMeta(fileName); 
    if (!logFile->HasKey(metaKey)) {
        return;
    }
    int64_t fileMetaSize = 
        mLogFile->GetValueLength(metaKey);
    if ((int64_t)mBuffer->GetBufferSize() < fileMetaSize)
    {
        delete mBuffer;
        mBuffer = new storage::FileBuffer(fileMetaSize);
    }
    int64_t valueLength = 
        mLogFile->Read(metaKey, mBuffer->GetBaseAddr(),
                       mBuffer->GetBufferSize());
    string metaStr(mBuffer->GetBaseAddr(), valueLength);
    FromJsonString(mFileMeta, metaStr);
    if (mFileMeta.maxBlockSize != mBuffer->GetBufferSize()) 
    {
        delete mBuffer;
        mBuffer = new storage::FileBuffer(mFileMeta.maxBlockSize);
    }
}

void ArchiveFile::Write(const void* buffer, size_t length) 
{
    const char* cursor = (const char*)buffer;
    size_t leftLen = length;
    while (true)
    {
        uint32_t leftLenInBuffer = mBuffer->GetBufferSize() - mBuffer->GetCursor();
        uint32_t lengthToWrite =  leftLenInBuffer < leftLen ? leftLenInBuffer : leftLen;
        mBuffer->CopyToBuffer(cursor, lengthToWrite);
        cursor += lengthToWrite;
        leftLen -= lengthToWrite;
        if (leftLen <= 0)
        {
            assert(leftLen == 0);
            break;
        }
        if (mBuffer->GetCursor() == mBuffer->GetBufferSize())
        {
            DumpBuffer();
        }
    }
}

std::string ArchiveFile::GetBlockKey(int64_t blockIdx) 
{
    return mFileName + "_" + mFileMeta.signature + "_" + 
        "archive_file_block_" + autil::StringUtil::toString(blockIdx);
}

void ArchiveFile::DumpBuffer()
{
    string blockKey = GetBlockKey(mFileMeta.blocks.size());
    Block block;
    block.key = blockKey;
    block.length = mBuffer->GetCursor();
    mFileMeta.blocks.push_back(block);
    mLogFile->Write(blockKey, mBuffer->GetBaseAddr(), mBuffer->GetCursor());
    mBuffer->SetCursor(0);
}

void ArchiveFile::Seek(int64_t offset, fslib::SeekFlag flag) 
{
    mOffset = offset;
}

int64_t ArchiveFile::Tell() 
{
    assert(false);
    return 0;
}

void ArchiveFile::Flush() 
{
    if (mBuffer->GetCursor() > 0) 
    {
        DumpBuffer();
        mLogFile->Flush();
    }
}

void ArchiveFile::Close() 
{
    if (mIsReadOnly)
    {
        return;
    }
    if (mBuffer->GetCursor() > 0) 
    {
        DumpBuffer();
    }
    string fileMeta = ToJsonString(mFileMeta);
    string fileMetaName = GetArchiveFileMeta(mFileName);
    mLogFile->Write(fileMetaName, fileMeta.c_str(), fileMeta.size());
    mLogFile->Flush();
}

int64_t ArchiveFile::GetFileLength() const
{
    int64_t totalLength = 0;
    for (int64_t i = 0; i < (int64_t)mFileMeta.blocks.size(); i++)
    {
        totalLength += mFileMeta.blocks[i].length;
    }
    return totalLength;
}

IE_NAMESPACE_END(storage);

