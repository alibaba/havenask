#include "indexlib/file_system/mmap_file_node_creator.h"
#include "indexlib/file_system/mmap_file_node.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace fslib;

IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MmapFileNodeCreator);

MmapFileNodeCreator::MmapFileNodeCreator()
    : mFullMemory(false)
{
}

MmapFileNodeCreator::~MmapFileNodeCreator() 
{
}

bool MmapFileNodeCreator::Init(const LoadConfig& loadConfig,
                               const util::BlockMemoryQuotaControllerPtr& memController)
{
    mMemController = memController;
    mLoadConfig = loadConfig;
    MmapLoadStrategyPtr loadStrategy = DYNAMIC_POINTER_CAST(MmapLoadStrategy,
            loadConfig.GetLoadStrategy());
    if (!loadStrategy)
    {
        IE_LOG(ERROR, "Load strategy dynamic fail.");
        return false;
    }
    mLock = loadStrategy->IsLock();
    mFullMemory = mLock || IsRemote();
    return true;
}

FileNodePtr MmapFileNodeCreator::CreateFileNode(
    const string& filePath, FSOpenType type, bool readOnly)
{
    assert(type == FSOT_MMAP || type == FSOT_LOAD_CONFIG);
    MmapFileNodePtr mmapFileNode(new MmapFileNode(mLoadConfig, mMemController, readOnly));
    return mmapFileNode;
}

bool MmapFileNodeCreator::Match(const string& filePath, const string& lifecycle) const
{
    return mLoadConfig.Match(filePath, lifecycle); 
}

bool MmapFileNodeCreator::MatchType(FSOpenType type) const
{
    return type == FSOT_MMAP || type == FSOT_LOAD_CONFIG;
}

bool MmapFileNodeCreator::IsRemote() const
{
    return mLoadConfig.IsRemote();
}

bool MmapFileNodeCreator::IsLock() const
{
    return mLock;
}

size_t MmapFileNodeCreator::EstimateFileLockMemoryUse(size_t fileLength) const
{
    return mFullMemory ? fileLength : 0;
}

bool MmapFileNodeCreator::OnlyPatialLock() const
{
    MmapLoadStrategyPtr loadStrategy = DYNAMIC_POINTER_CAST(MmapLoadStrategy,
            mLoadConfig.GetLoadStrategy());
    if (!loadStrategy)
    {
        IE_LOG(ERROR, "Load strategy dynamic fail.");
        return false;
    }
    if (loadStrategy->IsPartialLock() && !loadStrategy->IsLock())
    {
        return true;
    }
    return false;
}



IE_NAMESPACE_END(file_system);

