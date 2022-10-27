#include "indexlib/file_system/buffered_file_node_creator.h"
#include "indexlib/file_system/buffered_file_node.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileNodeCreator);

BufferedFileNodeCreator::BufferedFileNodeCreator(SessionFileCachePtr fileCache)
    : mFileCache(fileCache)
{
}

BufferedFileNodeCreator::~BufferedFileNodeCreator() 
{
}

bool BufferedFileNodeCreator::Init(const LoadConfig& loadConfig,
                                   const util::BlockMemoryQuotaControllerPtr& memController)
{
    mMemController = memController;
    return true;
}

FileNodePtr BufferedFileNodeCreator::CreateFileNode(
    const string& filePath, FSOpenType type, bool readOnly)
{
    assert(type == FSOT_BUFFERED);
    assert(readOnly);

    FileNodePtr bufferedFileNode(new BufferedFileNode(true, mFileCache));
    return bufferedFileNode;
}

bool BufferedFileNodeCreator::Match(const string& filePath, const string& lifecycle) const

{
    // load config list not support buffered file reader
    assert(false);
    return false;
}

bool BufferedFileNodeCreator::MatchType(FSOpenType type) const
{
    assert(false);
    return false;
}

size_t BufferedFileNodeCreator::EstimateFileLockMemoryUse(size_t fileLength) const
{
    return 0;
}

IE_NAMESPACE_END(file_system);

