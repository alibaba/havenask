#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/file_system/block_file_node.h"
#include "indexlib/file_system/block_file_node_creator.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BlockFileNodeCreator);

BlockFileNodeCreator::BlockFileNodeCreator(
        const FileBlockCachePtr& fileBlockCache)
    : mFileBlockCache(fileBlockCache)
    , mUseDirectIO(false)
    , mCacheDecompressFile(false)
{
}

BlockFileNodeCreator::~BlockFileNodeCreator() 
{
    if (mFileBlockCache && mFileBlockCache->GetBlockCache())
    {
        IE_LOG(DEBUG, "cache [%s] max block count [%u], used block count [%u]", 
               mLoadConfig.GetName().c_str(), 
               mFileBlockCache->GetBlockCache()->GetMaxBlockCount(),
               mFileBlockCache->GetBlockCache()->GetBlockCount());
    }
}

bool BlockFileNodeCreator::Init(const LoadConfig& loadConfig,
                                const util::BlockMemoryQuotaControllerPtr& memController)
{
    mLoadConfig = loadConfig;
    mMemController = memController;
    CacheLoadStrategyPtr loadStrategy = DYNAMIC_POINTER_CAST(CacheLoadStrategy,
            loadConfig.GetLoadStrategy());
    assert(loadStrategy);
    mUseDirectIO = loadStrategy->UseDirectIO();
    mCacheDecompressFile = loadStrategy->CacheDecompressFile();
    if (loadStrategy->UseGlobalCache())
    {
        if (mFileBlockCache)
        {
            return true;
        }
        IE_LOG(ERROR, "load config[%s] need global file block cache",
               loadConfig.GetName().c_str());
        return false;
    }

    assert(!mFileBlockCache);
    mFileBlockCache.reset(new FileBlockCache());
    util::SimpleMemoryQuotaControllerPtr quotaController(
            new util::SimpleMemoryQuotaController(mMemController));

    return mFileBlockCache->Init(loadStrategy->GetCacheSize(),
                                 loadStrategy->GetBlockSize(), quotaController);
}

FileNodePtr BlockFileNodeCreator::CreateFileNode(
    const string& filePath, FSOpenType type, bool readOnly)
{
    assert(type == FSOT_CACHE || type == FSOT_LOAD_CONFIG);
    assert(readOnly);
    assert(mFileBlockCache);
    util::BlockCache* blockCache = mFileBlockCache->GetBlockCache().get();
    assert(blockCache);
    
    IE_LOG(DEBUG, "cache [%s] max block count [%u], used block count [%u], block size [%u]", 
           mLoadConfig.GetName().c_str(), blockCache->GetMaxBlockCount(),
           blockCache->GetBlockCount(), blockCache->GetBlockSize());

    BlockFileNodePtr blockFileNode(new BlockFileNode(
                    blockCache, mUseDirectIO, mCacheDecompressFile));
    return blockFileNode;
}

bool BlockFileNodeCreator::Match(const string& filePath, const string& lifecycle) const
{
    return mLoadConfig.Match(filePath, lifecycle);
}

bool BlockFileNodeCreator::MatchType(FSOpenType type) const
{
    return (type == FSOT_CACHE || type == FSOT_LOAD_CONFIG);
}

bool BlockFileNodeCreator::IsRemote() const
{
    return mLoadConfig.IsRemote();
}

IE_NAMESPACE_END(file_system);

