#ifndef __INDEXLIB_BLOCK_FILE_NODE_CREATOR_H
#define __INDEXLIB_BLOCK_FILE_NODE_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node_creator.h"
#include "indexlib/file_system/block_cache_metrics.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"

IE_NAMESPACE_BEGIN(file_system);

class BlockFileNodeCreator : public FileNodeCreator
{
public:
    BlockFileNodeCreator(const FileBlockCachePtr& fileBlockCache = FileBlockCachePtr());
    ~BlockFileNodeCreator();

public:
    bool Init(const LoadConfig& loadConfig,
              const util::BlockMemoryQuotaControllerPtr& memController) override;
    FileNodePtr CreateFileNode(const std::string& filePath,
                               FSOpenType type, bool readOnly) override;
    bool Match(const std::string& filePath, const std::string& lifecycle) const override;
    bool MatchType(FSOpenType type) const override;
    bool IsRemote() const override;
    FSOpenType GetDefaultOpenType() const override
    { return FSOT_CACHE; }
    size_t EstimateFileLockMemoryUse(size_t fileLength) const override
    { return 0; }
    // If dictionary is loaded with block cache, then block index is always locked.
    bool OnlyPatialLock() const override { return true; }
private:
    LoadConfig mLoadConfig;
    FileBlockCachePtr mFileBlockCache;
    bool mUseDirectIO;
    bool mCacheDecompressFile;
    
private:
    IE_LOG_DECLARE();
    friend class BlockFileNodeCreatorTest;
    friend class LocalStorageTest;
};

DEFINE_SHARED_PTR(BlockFileNodeCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCK_FILE_NODE_CREATOR_H
