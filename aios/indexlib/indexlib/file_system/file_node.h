#ifndef __INDEXLIB_FILE_NODE_H
#define __INDEXLIB_FILE_NODE_H

#include <tr1/memory>
#include <future_lite/Future.h>
#include <fslib/common/common_type.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/package_file_meta.h"
#include "indexlib/file_system/read_option.h"
#include "indexlib/file_system/package_open_meta.h"

IE_NAMESPACE_BEGIN(file_system);


class FileNode
{
public:
    FileNode();
    FileNode(const FileNode& other);
    virtual ~FileNode();

public:
    // for single file open
    void Open(const std::string& path, FSOpenType openType);

    // for inner File open in package file
    void Open(const PackageOpenMeta& packageOpenMeta, FSOpenType openType);

    // for single file open with FileMeta
    void Open(const std::string& path, const fslib::FileMeta& fileMeta, FSOpenType openType);
    // for hybrid storage file open with FileMeta
    void Open(const std::string& path, const std::string& realPath,
              const fslib::FileMeta& fileMeta, FSOpenType openType);

public:
    // load file into memory
    virtual void Populate() { }
    virtual FSFileType GetType() const = 0;
    virtual size_t GetLength() const = 0;
    virtual void* GetBaseAddress() const = 0;
    virtual size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) = 0;
    virtual future_lite::Future<size_t>
    ReadAsync(void* buffer, size_t length, size_t offset, ReadOption option)
    {
        return future_lite::makeReadyFuture(Read(buffer, length, offset, option));
    }
    virtual future_lite::Future<uint32_t> ReadUInt32Async(size_t offset, ReadOption option)
    {
        uint32_t buffer;
        auto readSize = Read(static_cast<void*>(&buffer), sizeof(buffer), offset, option);
        assert(readSize == sizeof(buffer));
        (void)readSize;
        return future_lite::makeReadyFuture<uint32_t>(buffer);
    }
    virtual util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) = 0;
    virtual size_t Write(const void* buffer, size_t length) = 0;
    virtual void Close() = 0;
    virtual FileNode* Clone() const;
    virtual void Lock(size_t offset, size_t length) {}
    virtual size_t Prefetch(size_t length, size_t offset, ReadOption option = ReadOption())
    { return 0; }

    virtual future_lite::Future<uint32_t> ReadVUInt32Async(size_t offset, ReadOption option);

    virtual bool ReadOnly() const = 0;

public:
    const std::string& GetPath() const { return mPath; }
    FSOpenType GetOpenType() const { return mOpenType; }
    bool MatchType(FSOpenType type, bool needWrite) const
    {
        if (needWrite && ReadOnly())
        {
            return false;
        }
        return (mOpenType == type) || (mOriginalOpenType == type);
    }
    void SetDirty(bool isDirty) { mDirty = isDirty; }
    bool IsDirty() const { return mDirty; }

    void SetInPackage(bool inPackage) { mInPackage = inPackage; }
    bool IsInPackage() const { return mInPackage; }

    void SetOriginalOpenType(FSOpenType type) { mOriginalOpenType = type; }
    FSOpenType GetOriginalOpenType() const { return mOriginalOpenType; }

private:
    virtual void DoOpen(const std::string& path, FSOpenType openType) = 0;
    virtual void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta, FSOpenType openType) = 0;
    virtual void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) = 0;

protected:
    std::string mPath;
    FSOpenType mOpenType;
    FSOpenType mOriginalOpenType;
    volatile bool mDirty;
    volatile bool mInPackage;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileNode);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_NODE_H
