#ifndef __INDEXLIB_BUFFERED_FILE_NODE_H
#define __INDEXLIB_BUFFERED_FILE_NODE_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include <fslib/fs/FileSystem.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/session_file_cache.h"

IE_NAMESPACE_BEGIN(file_system);

class BufferedFileNode : public FileNode
{
public:
    BufferedFileNode(bool readOnly = true,
                     const SessionFileCachePtr& fileCache = SessionFileCachePtr());
    ~BufferedFileNode();

public:
    FSFileType GetType() const override;
    size_t GetLength() const override;
    void* GetBaseAddress() const override;
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override;
    size_t Write(const void* buffer, size_t length) override;
    void Close() override;
    bool ReadOnly() const override { return mReadOnly; }

private:
    void DoOpen(const std::string& path, FSOpenType openType) override;
    void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta, FSOpenType openType) override;
    void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) override;

private:
    // virtual for test
    virtual fslib::fs::FilePtr CreateFile(
        const std::string& fileName, fslib::Flag mode, bool useDirectIO,
        ssize_t physicalFileLength, size_t beginOffset, ssize_t fileLength);
    void IOCtlPrefetch(const fslib::fs::FilePtr& file, ssize_t blockSize,
                       size_t beginOffset, ssize_t fileLength);
    void CheckError();
private:
    static const uint32_t DEFAULT_READ_WRITE_LENGTH = 64 * 1024 * 1024;

private:
    size_t mLength;
    size_t mFileBeginOffset;
    bool mReadOnly;
    std::string mOriginalFileName;
    fslib::fs::FilePtr mFile;
    SessionFileCachePtr mFileCache;

private:
    friend class BufferedFileWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BufferedFileNode);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BUFFERED_FILE_NODE_H
