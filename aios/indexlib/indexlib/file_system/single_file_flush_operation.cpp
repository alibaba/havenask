#include <autil/TimeUtility.h>
#include "indexlib/file_system/single_file_flush_operation.h"
#include "indexlib/file_system/dir_operation_cache.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/file_wrapper.h"

using namespace std;
using namespace autil;
using namespace fslib;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, SingleFileFlushOperation);

SingleFileFlushOperation::SingleFileFlushOperation(
    const FileNodePtr& fileNode, const FSDumpParam& param)
    : mDestPath(fileNode->GetPath())
    , mFileNode(fileNode)
    , mDumpParam(param)
{
    assert(mFileNode);
    if (param.copyOnDump)
    {
        IE_LOG(INFO, "CopyFileNode for dump %s", mFileNode->GetPath().c_str());
        mFlushFileNode.reset(mFileNode->Clone());
        mFlushMemoryUse += mFlushFileNode->GetLength();
    }
    else
    {
        mFlushFileNode = mFileNode;
    }
}

SingleFileFlushOperation::~SingleFileFlushOperation()
{
}

void SingleFileFlushOperation::Execute(const PathMetaContainerPtr& pathMetaContainer)
{
    if (mDumpParam.atomicDump)
    {
        AtomicStore(mDestPath);
    }
    else
    {
        Store(mDestPath);
    }
    
    if (pathMetaContainer)
    {
        uint64_t time = TimeUtility::currentTimeInSeconds();
        pathMetaContainer->AddFileInfo(mDestPath, mFileNode->GetLength(), time, false);
        pathMetaContainer->AddFileInfo(DirOperationCache::GetParent(mDestPath), -1, time, true);
    }
    mFileNode->SetDirty(false);
}

void SingleFileFlushOperation::Store(const string& filePath)
{
    assert(mFlushFileNode);
    assert(mFlushFileNode->GetType() == FSFT_IN_MEM);
    void* data = mFlushFileNode->GetBaseAddress();
    size_t dataLen = mFlushFileNode->GetLength();

    string path = filePath;
    if (mDumpParam.raidConfig && mDumpParam.raidConfig->useRaid
        && dataLen > mDumpParam.raidConfig->bufferSizeThreshold)
    {
        path = mDumpParam.raidConfig->MakeRaidPath(filePath);
    }

    // will make parent dir, so we do not check dir exists
    FileWrapperPtr file(FileSystemWrapper::OpenFile(path, fslib::WRITE));
    if (dataLen > 0)
    {
        assert(data);
        SplitWrite(file, data, dataLen);
    }
    IE_LOG(INFO, "flush file[%s], len=%luB", path.c_str(), dataLen);
    file->Close();
}

void SingleFileFlushOperation::AtomicStore(const string& filePath)
{
    if (FileSystemWrapper::IsExistIgnoreError(filePath))
    {
        INDEXLIB_FATAL_ERROR(Exist, "file [%s] already exist.",
                             filePath.c_str());
    }
    string tempFilePath = filePath + TEMP_FILE_SUFFIX;
    if (FileSystemWrapper::IsExistIgnoreError(tempFilePath))
    {
        IE_LOG(WARN, "Remove temp file: %s.", tempFilePath.c_str());
        FileSystemWrapper::DeleteFile(tempFilePath);
    }

    Store(tempFilePath);

    FileSystemWrapper::Rename(tempFilePath, filePath);
}

void SingleFileFlushOperation::GetFileNodePaths(FileList& fileList) const
{
    fileList.push_back(mFileNode->GetPath());
}

IE_NAMESPACE_END(file_system);
