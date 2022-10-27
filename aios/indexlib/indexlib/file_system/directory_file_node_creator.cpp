#include "indexlib/file_system/directory_file_node_creator.h"
#include "indexlib/file_system/directory_file_node.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, DirectoryFileNodeCreator);

DirectoryFileNodeCreator::DirectoryFileNodeCreator()
{
}

DirectoryFileNodeCreator::~DirectoryFileNodeCreator() 
{
}

bool DirectoryFileNodeCreator::Init(const LoadConfig& loadConfig,
                                    const util::BlockMemoryQuotaControllerPtr& memController)
{
    mMemController = memController;
    return true;
}

FileNodePtr DirectoryFileNodeCreator::CreateFileNode(const string& filePath,
                                                     FSOpenType type, bool readOnly)
{
    assert(type == FSOT_UNKNOWN);
    assert(readOnly);

    FileNodePtr fileNode(new DirectoryFileNode());
    fileNode->Open(filePath, type);

    return fileNode;
}

bool DirectoryFileNodeCreator::Match(const string& filePath, const std::string& lifecycle) const
{
    assert(false);
    return false;
}

bool DirectoryFileNodeCreator::MatchType(FSOpenType type) const
{
    assert(false);
    return false;
}

size_t DirectoryFileNodeCreator::EstimateFileLockMemoryUse(size_t fileLength) const
{
    return 0;
}

IE_NAMESPACE_END(file_system);

