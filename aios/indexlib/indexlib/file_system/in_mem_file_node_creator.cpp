#include "indexlib/file_system/in_mem_file_node_creator.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace fslib;

IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemFileNodeCreator);

InMemFileNodeCreator::InMemFileNodeCreator(SessionFileCachePtr fileCache)
    : mFileCache(fileCache)
{
}

InMemFileNodeCreator::~InMemFileNodeCreator() 
{
}

bool InMemFileNodeCreator::Init(const LoadConfig& loadConfig,
                                const util::BlockMemoryQuotaControllerPtr& memController)
{
    mMemController = memController;
    mLoadConfig = loadConfig;
    return true;
}

FileNodePtr InMemFileNodeCreator::CreateFileNode(const string& filePath,
                                                 FSOpenType type, bool readOnly)
{
    assert(type == FSOT_IN_MEM || type == FSOT_LOAD_CONFIG || type == FSOT_MMAP);

    InMemFileNodePtr inMemFileNode(
        new InMemFileNode(0, true, mLoadConfig, mMemController, mFileCache));
    return inMemFileNode;
}

bool InMemFileNodeCreator::Match(const string& filePath, const string& lifecycle) const
{
    return mLoadConfig.Match(filePath, lifecycle); 
}

bool InMemFileNodeCreator::MatchType(FSOpenType type) const
{
    return (type == FSOT_IN_MEM || type == FSOT_LOAD_CONFIG);
}

bool InMemFileNodeCreator::IsRemote() const
{
    return mLoadConfig.IsRemote();
}

size_t InMemFileNodeCreator::EstimateFileLockMemoryUse(size_t fileLength) const
{
    return fileLength;
}

IE_NAMESPACE_END(file_system);

