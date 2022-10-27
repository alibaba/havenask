#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include "indexlib/file_system/package_file_flush_operation.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/dir_operation_cache.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/file_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PackageFileFlushOperation);

PackageFileFlushOperation::PackageFileFlushOperation(
        const string& packageFilePath,
        const PackageFileMetaPtr& packageFileMeta,
        const vector<FileNodePtr>& fileNodeVec,
        const vector<FSDumpParam>& dumpParamVec,
        const storage::RaidConfigPtr& raidConfig)
    : mPackageFilePath(packageFilePath)
    , mPackageFileMeta(packageFileMeta)
    , mInnerFileNodes(fileNodeVec)
    , mAtomicDump(false)
{
    assert(mPackageFileMeta);
    assert(fileNodeVec.size() == dumpParamVec.size());
    mFlushFileNodes.reserve(fileNodeVec.size());
    
    for (size_t i = 0; i < fileNodeVec.size(); ++i)
    {
        mAtomicDump = mAtomicDump || dumpParamVec[i].atomicDump;
        FileNodePtr flushFileNode = fileNodeVec[i];
        if (dumpParamVec[i].copyOnDump)
        {
            IE_LOG(INFO, "CopyFileNode for dump %s", fileNodeVec[i]->GetPath().c_str());
            flushFileNode.reset(fileNodeVec[i]->Clone());
            mFlushMemoryUse += flushFileNode->GetLength();
        }
        mFlushFileNodes.push_back(flushFileNode);
    }
    mRaidConfig = raidConfig;
}

PackageFileFlushOperation::~PackageFileFlushOperation() 
{
}

void PackageFileFlushOperation::Execute(const PathMetaContainerPtr& pathMetaContainer)
{
    string packageFileMetaPath = mPackageFilePath + PACKAGE_FILE_META_SUFFIX;
    if (mAtomicDump)
    {
        if (FileSystemWrapper::IsExistIgnoreError(packageFileMetaPath))
        {
            INDEXLIB_FATAL_ERROR(Exist, "file [%s] already exist.",
                                 packageFileMetaPath.c_str());
        }   
    }
    StoreDataFile(pathMetaContainer);
    if (mAtomicDump)
    {
        string tempMetaPath = packageFileMetaPath + TEMP_FILE_SUFFIX;
        if (FileSystemWrapper::IsExistIgnoreError(tempMetaPath))
        {
            IE_LOG(WARN, "Remove temp file: %s.", tempMetaPath.c_str());
            FileSystemWrapper::DeleteFile(tempMetaPath);
        }
        StoreMetaFile(tempMetaPath);
        FileSystemWrapper::Rename(tempMetaPath, packageFileMetaPath);
    }
    else
    {
        StoreMetaFile(packageFileMetaPath);
    }

    if (pathMetaContainer)
    {
        uint64_t time = TimeUtility::currentTimeInSeconds();
        string metaContent = mPackageFileMeta->ToString();
        pathMetaContainer->AddFileInfo(packageFileMetaPath, metaContent.size(), time, false);
        pathMetaContainer->AddFileInfo(DirOperationCache::GetParent(
                        packageFileMetaPath), -1, time, true);
    }
    MakeFlushedFileNotDirty();
}

bool PackageFileFlushOperation::NeedRaid() const {
    if (!mRaidConfig || !mRaidConfig->useRaid)
    {
        return false;
    }
    size_t dataFileLen = 0;
    size_t innerFileIdx = 0;    
    for (auto iter = mPackageFileMeta->Begin(); iter != mPackageFileMeta->End(); iter++)
    {
        const auto& innerFileMeta = *iter;
        if (innerFileMeta.IsDir())
        {
            continue;
        }
        dataFileLen += (innerFileMeta.GetOffset() - dataFileLen);
        const auto& innerFileNode = mFlushFileNodes[innerFileIdx++];
        dataFileLen += innerFileNode->GetLength();
        if (dataFileLen > mRaidConfig->bufferSizeThreshold)
        {
            return true;
        }
    }
    return false;
}

void PackageFileFlushOperation::StoreDataFile(
        const PathMetaContainerPtr& pathMetaContainer)
{
    size_t dataFileIdx = 0;
    string packageFileDataPath = PackageFileMeta::GetPackageFileDataPath(
        mPackageFilePath, "", dataFileIdx);

    string physicalPath = packageFileDataPath;
    if (NeedRaid())
    {
        physicalPath = mRaidConfig->MakeRaidPath(packageFileDataPath);
    }

    FileWrapperPtr dataFile(FileSystemWrapper::OpenFile(
                    physicalPath, fslib::WRITE));

    size_t dataFileLen = 0;
    size_t innerFileIdx = 0;
    vector<char> paddingBuffer(mPackageFileMeta->GetFileAlignSize(), 0);
    PackageFileMeta::Iterator iter = mPackageFileMeta->Begin();
    for ( ; iter != mPackageFileMeta->End(); iter++)
    {
        const PackageFileMeta::InnerFileMeta& innerFileMeta = *iter;
        if (innerFileMeta.IsDir())
        {
            continue;
        }

        size_t paddingLen = innerFileMeta.GetOffset() - dataFileLen;
        dataFile->Write(paddingBuffer.data(), paddingLen);
        dataFileLen += paddingLen;

        const FileNodePtr& innerFileNode = mFlushFileNodes[innerFileIdx++];
        assert(innerFileNode->GetType() == FSFT_IN_MEM);

        void* data = innerFileNode->GetBaseAddress();
        size_t dataLen = innerFileNode->GetLength();
        assert(innerFileMeta.GetLength() == dataLen);

        SplitWrite(dataFile, data, dataLen);
        dataFileLen += dataLen;
    }
    assert(innerFileIdx == mFlushFileNodes.size());
    dataFile->Close();
    IE_LOG(INFO, "flush file[%s], len=%luB", physicalPath.c_str(), dataFileLen);

    if (pathMetaContainer)
    {
        uint64_t time = TimeUtility::currentTimeInSeconds();
        pathMetaContainer->AddFileInfo(packageFileDataPath, dataFileLen, time, false);
        pathMetaContainer->AddFileInfo(DirOperationCache::GetParent(
                        packageFileDataPath), -1, time, true);
    }
}

void PackageFileFlushOperation::StoreMetaFile(const string& metaFilePath)
{
    string metaContent = mPackageFileMeta->ToString();
    string physicalPath = metaFilePath;
    if (mRaidConfig && mRaidConfig->useRaid
        && metaContent.size() > mRaidConfig->bufferSizeThreshold)
    {
        physicalPath = mRaidConfig->MakeRaidPath(metaFilePath);
    }
    FileWrapperPtr metaFile(FileSystemWrapper::OpenFile(physicalPath, fslib::WRITE));
    metaFile->Write(metaContent.c_str(), metaContent.size());
    metaFile->Close();
    IE_LOG(INFO, "flush file[%s], len=%luB", physicalPath.c_str(), metaContent.size());
}

void PackageFileFlushOperation::MakeFlushedFileNotDirty()
{
    for (size_t i = 0; i < mInnerFileNodes.size(); i++)
    {
        mInnerFileNodes[i]->SetDirty(false);
    }
}

void PackageFileFlushOperation::GetFileNodePaths(FileList& fileList) const
{
    for (size_t i = 0; i < mInnerFileNodes.size(); i++)
    {
        fileList.push_back(mInnerFileNodes[i]->GetPath());
    }
}

IE_NAMESPACE_END(file_system);

