#ifndef __INDEXLIB_ARCHIVE_FILE_H
#define __INDEXLIB_ARCHIVE_FILE_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/file_wrapper.h"
#include "indexlib/storage/log_file.h"

IE_NAMESPACE_BEGIN(storage);

class ArchiveFile : public storage::FileWrapper
{
public:
    ArchiveFile(bool isReadOnly, const std::string& signature)
        : storage::FileWrapper(NULL)
        , mOffset(0)
        , mCurrentBlock(-1)
        , mIsReadOnly(isReadOnly)
    {
        mFileMeta.maxBlockSize = 4 * 1024 * 1024; //default 4M
        mFileMeta.signature = signature;
        mBuffer = new storage::FileBuffer(mFileMeta.maxBlockSize);
    }
    ~ArchiveFile();
private:
    static std::string GetArchiveFileMeta(const std::string& fileName) 
    {
        return fileName + "_" + "archive_file_meta";
    }

public:
    void Open(const std::string& fileName, const LogFilePtr& logFile); 
    size_t Read(void* buffer, size_t length) override;
    size_t PRead(void* buffer, size_t length, off_t offset) override;
    void Write(const void* buffer, size_t length) override;
    void Flush() override;
    void Close() override;
    static bool IsExist(const std::string& innerFileName, const LogFilePtr& logFile)
    {
        std::string fileMetaName = GetArchiveFileMeta(innerFileName);
        return logFile->HasKey(fileMetaName);
    }
    const char* GetFileName() const
    {
        return mFileName.c_str();
    }
    
    int64_t GetFileLength() const;

private:
    ArchiveFile(int64_t bufferSize)
        : storage::FileWrapper(NULL)
        , mBuffer(new storage::FileBuffer(bufferSize))
        , mOffset(0)
        , mCurrentBlock(-1)
    {
    }
    void Seek(int64_t offset, fslib::SeekFlag flag) override;
    int64_t Tell() override;
    void DumpBuffer();

    struct Block : public autil::legacy::Jsonizable {
        std::string key;
        int64_t length;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) 
        {
            json.Jsonize("block_key", key, key);
            json.Jsonize("block_length", length, length);
        }
    };

    struct FileMeta : public autil::legacy::Jsonizable {
        int64_t maxBlockSize;
        std::string signature;
        std::vector<Block> blocks;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) 
        {
            json.Jsonize("max_block_size", maxBlockSize, maxBlockSize);
            json.Jsonize("blocks", blocks, blocks);
            json.Jsonize("signature", signature, signature);
        }
    };

    struct ReadBlockInfo 
    {
        int64_t beginPos;
        int64_t readLength;
        int64_t blockIdx;
    };

private:
    std::string GetBlockKey(int64_t blockIdx);
    void CalculateBlockInfo(int64_t offset, size_t length,
                            std::vector<ReadBlockInfo>& readInfos);
    void FillReadBuffer(int64_t blockIdx);
    void FillBufferFromBlock(const ReadBlockInfo& readInfo, char* buffer);

    //ForTest
    void ResetBufferSize(int64_t bufferSize)
    {
        if (mBuffer) 
        {
            delete mBuffer;
        }
        mFileMeta.maxBlockSize = bufferSize;
        mBuffer = new storage::FileBuffer(bufferSize);
    }
    int64_t GetBufferSize() { return mBuffer->GetBufferSize(); }

private:
    friend class ArchiveFolderTest;

private:
    std::string mFileName;
    LogFilePtr mLogFile;
    storage::FileBuffer *mBuffer;
    FileMeta mFileMeta;
    int64_t mOffset;
    int64_t mCurrentBlock;
    bool mIsReadOnly;
    std::string mSignature;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ArchiveFile);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_ARCHIVE_FILE_H
