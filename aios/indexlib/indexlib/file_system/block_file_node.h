#ifndef __INDEXLIB_BLOCK_FILE_NODE_H
#define __INDEXLIB_BLOCK_FILE_NODE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/block_file_accessor.h"

DECLARE_REFERENCE_CLASS(util, BlockCache);
DECLARE_REFERENCE_CLASS(util, BlockAllocator);

IE_NAMESPACE_BEGIN(file_system);

class BlockFileNode final: public FileNode
{
public:
    BlockFileNode(util::BlockCache* blockCache,
                  bool useDirectIO = false, bool cacheDecompressFile = false);
    ~BlockFileNode();

public:
    FSFileType GetType() const override;
    size_t GetLength() const override;
    void* GetBaseAddress() const override;
    size_t Read(
        void* buffer, size_t length, size_t offset, ReadOption option = ReadOption()) override;

    future_lite::Future<size_t>
    ReadAsync(void* buffer, size_t length, size_t offset, ReadOption option) override;
    util::ByteSliceList* Read(size_t length, size_t offset, ReadOption option = ReadOption()) override;
    size_t Write(const void* buffer, size_t length) override;
    void Close() override;
    size_t Prefetch(size_t length, size_t offset, ReadOption option = ReadOption()) override;
    future_lite::Future<uint32_t> ReadUInt32Async(size_t offset, ReadOption option) override;
    bool ReadOnly() const override { return true; }

public:
    bool EnableCacheDecompressFile() const { return mCacheDecompressFile; }
    util::BlockCache* GetBlockCache() const { return mAccessor.GetBlockCache(); }
    BlockFileAccessor* GetAccessor() { return &mAccessor; }
    
private:
    void DoOpen(const std::string& path, FSOpenType openType) override;
    void DoOpen(const std::string& path, const fslib::FileMeta& fileMeta,
                FSOpenType openType) override;
    void DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) override;
    future_lite::Future<uint32_t> DoReadUInt32Async(
        size_t offset, size_t leftBytes, uint32_t currentValue, ReadOption option);
private:
    BlockFileAccessor mAccessor;
    bool mCacheDecompressFile;

private:
    IE_LOG_DECLARE();
    friend class BlockFileNodeTest;
};

DEFINE_SHARED_PTR(BlockFileNode);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCK_FILE_NODE_H
