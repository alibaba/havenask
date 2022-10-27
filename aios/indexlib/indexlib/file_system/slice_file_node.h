#ifndef __INDEXLIB_SLICE_FILE_NODE_H
#define __INDEXLIB_SLICE_FILE_NODE_H

#include <tr1/memory>
#include <vector>
#include <indexlib/util/mmap_allocator.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/storage_metrics.h"
#include "indexlib/util/slice_array/bytes_aligned_slice_array.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"
// ATTENTION: thread-safe only when 
// totalSize < reservedSliceNum * sliceLen
// ATTENTION: user must ensure sliceLen >= MaxValueLength

IE_NAMESPACE_BEGIN(file_system);

class FSSliceMemoryMetrics : public util::SliceMemoryMetrics
{
public:
    FSSliceMemoryMetrics(StorageMetrics* storageMetrics)
        : mStorageMetrics(storageMetrics)
    {}

    void Increase(size_t size) override
    {
        if (mStorageMetrics)
        {
            mStorageMetrics->IncreaseFileLength(FSFT_SLICE, size);
        }
    }
    
    void Decrease(size_t size) override
    {
        if (mStorageMetrics)
        {
            mStorageMetrics->DecreaseFileLength(FSFT_SLICE, size);
        }
    }

private:
    StorageMetrics* mStorageMetrics;
};


class SliceFileNode : public FileNode
{
public:
    typedef std::vector<uint8_t*> SlicePrtVec;

public:
    SliceFileNode(uint64_t sliceLen, int32_t sliceNum, 
                  const util::BlockMemoryQuotaControllerPtr& memController);
    ~SliceFileNode();

public:
    FSFileType GetType() const override;
    size_t GetLength() const override;
    void* GetBaseAddress() const override;
    size_t Read(void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override;
    size_t Write(const void* buffer, size_t length) override;
    void Close() override;
    bool ReadOnly() const override { return false; }

public:
    const util::BytesAlignedSliceArrayPtr& GetBytesAlignedSliceArray() const
    { return mBytesAlignedSliceArray; }

    void SetStorageMetrics(StorageMetrics* storageMetrics)
    {
        util::SliceMemoryMetricsPtr metrics(new FSSliceMemoryMetrics(storageMetrics));
        mBytesAlignedSliceArray->SetSliceMemoryMetrics(metrics);
    }

    size_t GetSliceFileLength() const
    { return mBytesAlignedSliceArray->GetTotalUsedBytes(); }

    static size_t EstimateFileLockMemoryUse(size_t fileLength)
    { return fileLength; }

private:
    void DoOpen(const std::string& path, FSOpenType openType) override;
    void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) override;
    void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta,
                FSOpenType openType) override;

private:
    util::BytesAlignedSliceArrayPtr mBytesAlignedSliceArray;
    int32_t mSliceNum;
private:
    IE_LOG_DECLARE();
    friend class SliceFileNodeTest;
};

DEFINE_SHARED_PTR(SliceFileNode);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SLICE_FILE_NODE_H
