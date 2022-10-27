#ifndef __INDEXLIB_IN_MEM_FILE_NODE_H
#define __INDEXLIB_IN_MEM_FILE_NODE_H

#include <tr1/memory>
#include <autil/Lock.h>
#include <autil/mem_pool/PoolBase.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"
#include "indexlib/file_system/load_config/in_mem_load_strategy.h"
#include "indexlib/file_system/load_config/load_config.h"
#include "indexlib/file_system/session_file_cache.h"
#include "indexlib/storage/file_wrapper.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemFileNode : public FileNode
{
public:
    InMemFileNode(size_t reservedSize, bool readFromDisk,
                  const LoadConfig& loadConfig,
                  const util::BlockMemoryQuotaControllerPtr& memController,
                  const SessionFileCachePtr& fileCache = SessionFileCachePtr());
    InMemFileNode(const InMemFileNode& other);
    ~InMemFileNode();

public:
    void Populate() override;
    FSFileType GetType() const override;
    size_t GetLength() const override;
    void* GetBaseAddress() const override;
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override;

    size_t Write(const void* buffer, size_t length) override;
    void Close() override;

    InMemFileNode* Clone() const override;

    inline size_t DoWrite(const void* buffer, size_t length);
    void Resize(size_t size);
    bool ReadOnly() const override { return false; }

private:
    void DoOpen(const std::string& path, FSOpenType openType) override;
    void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) override;
    void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta,
                FSOpenType openType) override;

    void DoOpenFromDisk(const std::string& path, size_t offset, size_t length, ssize_t fileLength);
    size_t CalculateSize(size_t needLength);
    void Extend(size_t extendSize);

    void AllocateMemQuota(size_t newLength)
    {
        assert(mMemController);
        if (newLength > mMemPeak)
        {
            mMemController->Allocate(newLength - mMemPeak);
            mMemPeak = newLength;
        }
    }

    void LoadData(const storage::FileWrapperPtr& file, int64_t offset, int64_t length);

private:
    uint8_t *mData;
    size_t mLength;
    size_t mMemPeak;
    size_t mCapacity;
    bool mReadFromDisk;
    autil::mem_pool::PoolBase *mPool;

    std::string mDataFilePath;
    size_t mDataFileOffset;
    ssize_t mDataFileLength;
    mutable autil::ThreadMutex mLock;
    bool mPopulated;
    util::SimpleMemoryQuotaControllerPtr mMemController;
    InMemLoadStrategyPtr mLoadStrategy;
    SessionFileCachePtr mFileCache;
private:
    IE_LOG_DECLARE();
    friend class InMemFileTest;
};

DEFINE_SHARED_PTR(InMemFileNode);
//////////////////////////////////////////////////////////////////

inline size_t InMemFileNode::DoWrite(const void* buffer, size_t length)
{
    assert(buffer);
    if (length + mLength > mCapacity)
    {
        Extend(CalculateSize(length + mLength));
    }
    assert(mData);
    memcpy(mData + mLength, buffer, length);
    mLength += length;
    AllocateMemQuota(mLength);
    return length;
}

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_IN_MEM_FILE_NODE_H
