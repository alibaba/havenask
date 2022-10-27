#ifndef __INDEXLIB_MMAP_FILE_NODE_H
#define __INDEXLIB_MMAP_FILE_NODE_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/load_config/load_config.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"

IE_NAMESPACE_BEGIN(file_system);

class MmapFileNode : public FileNode
{
public:
    MmapFileNode(const LoadConfig& loadConfig,
                 const util::BlockMemoryQuotaControllerPtr& memController,
                 bool readOnly);
    ~MmapFileNode();

public:
    void Populate() override;
    FSFileType GetType() const override;
    size_t GetLength() const override;
    void* GetBaseAddress() const override;
    size_t Read(void* buffer, size_t length, size_t offset,
                ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset,
                              ReadOption option = ReadOption()) override;
    size_t Write(const void* buffer, size_t length) override;
    void Close() override;
    void Lock(size_t offset, size_t length) override;
    future_lite::Future<uint32_t> ReadVUInt32Async(size_t offset, ReadOption option) override;

    bool ReadOnly() const override { return mReadOnly; };

protected:
    void LoadData();
    void DoOpen(const std::string& path, FSOpenType openType) override;

private:
    void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) override;
    // you should make sure fileMeta is reliable
    void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta, FSOpenType openType) override;

private:
    void DoOpenMmapFile(const std::string& path, size_t offset,
                        size_t length, FSOpenType openType, ssize_t fileLength);
    // multiple file share one mmaped shared package file
    void DoOpenInSharedFile(const std::string& path, size_t offset, size_t length,
                            const std::shared_ptr<fslib::fs::MMapFile>& sharedFile,
                            FSOpenType openType);
    uint8_t WarmUp(const char *addr, int64_t len);

protected:
    mutable autil::ThreadMutex mLock;
    std::shared_ptr<fslib::fs::MMapFile> mFile;
    util::SimpleMemoryQuotaControllerPtr mMemController;
    MmapLoadStrategyPtr mLoadStrategy;
    FileNodePtr mDependFileNode; // dcache, for hold shared package file
    void* mData;
    size_t mLength;
    FSFileType mType;
    bool mWarmup;
    bool mPopulated;
    bool mReadOnly;

private:
    IE_LOG_DECLARE();
    friend class MmapFileNodeTest;
};

DEFINE_SHARED_PTR(MmapFileNode);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MMAP_FILE_NODE_H
