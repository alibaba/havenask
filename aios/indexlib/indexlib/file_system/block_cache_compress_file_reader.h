#ifndef __INDEXLIB_BLOCK_CACHE_COMPRESS_FILE_READER_H
#define __INDEXLIB_BLOCK_CACHE_COMPRESS_FILE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/compress_file_reader.h"
#include "indexlib/file_system/block_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class BlockCacheCompressFileReader : public CompressFileReader
{
public:
    BlockCacheCompressFileReader(autil::mem_pool::Pool* pool = NULL)
        : CompressFileReader(pool)
        , mFileId(0)
    {}
    
    ~BlockCacheCompressFileReader() {}
    
public:
    bool Init(const FileReaderPtr& fileReader,
              const CompressFileInfo& compressInfo, Directory* directory) override;

    BlockCacheCompressFileReader* CreateSessionReader(
            autil::mem_pool::Pool* pool) override;

    void Close() override
    {
        CompressFileReader::Close();
        mBlockFileNode.reset();
        mFileId = 0;
    };

private:
    void LoadBuffer(size_t offset, ReadOption option) override;

    void DecompressBuffer(size_t blockIdx, ReadOption option);
    void PutInCache(util::BlockCache* blockCache,
                    const util::blockid_t& blockId, util::Cache::Handle **handle);

private:
    BlockFileNodePtr mBlockFileNode;
    uint64_t mFileId;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BlockCacheCompressFileReader);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCK_CACHE_COMPRESS_FILE_READER_H
