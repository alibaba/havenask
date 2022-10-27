#include "indexlib/file_system/mkdir_flush_operation.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, MkdirFlushOperation);

MkdirFlushOperation::MkdirFlushOperation(const FileNodePtr& fileNode)
    : mDestPath(fileNode->GetPath())
    , mFileNode(fileNode)
{
    assert(fileNode);
}

MkdirFlushOperation::~MkdirFlushOperation() 
{
}

void MkdirFlushOperation::Execute()
{
    assert(!mDestPath.empty());
    if (!FileSystemWrapper::IsExist(mDestPath))
    {
        FileSystemWrapper::MkDirIfNotExist(mDestPath);
    }
}

IE_NAMESPACE_END(file_system);

