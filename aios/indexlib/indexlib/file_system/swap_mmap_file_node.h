#ifndef __INDEXLIB_SWAP_MMAP_FILE_NODE_H
#define __INDEXLIB_SWAP_MMAP_FILE_NODE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/mmap_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class SwapMmapFileNode : public MmapFileNode
{
public:
    SwapMmapFileNode(size_t fileSize,
                     const util::BlockMemoryQuotaControllerPtr& memController);
    ~SwapMmapFileNode();

public:
    size_t GetLength() const override { return mCursor; }

    void Lock(size_t offset, size_t length) override;
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override;
    size_t Write(const void* buffer, size_t length) override;

    void OpenForRead(const std::string& path, FSOpenType openType);
    void OpenForWrite(const std::string& path, FSOpenType openType);
    bool ReadOnly() const override { return mReadOnly; }

public:
    void WarmUp();
    void Resize(size_t size);
    size_t GetCapacity() const { return mLength; }
    void Sync();
    void SetRemainFile() { mRemainFile = true; }
    
private:
    void DoOpen(const std::string& path, FSOpenType openType) override;
    void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) override;
    void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta, FSOpenType openType) override;
private:
    size_t mCursor;
    volatile bool mRemainFile;
    bool mReadOnly;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SwapMmapFileNode);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SWAP_MMAP_FILE_NODE_H
