#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/env_util.h"
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/package_storage.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, MergeTaskResourceManager);

MergeTaskResourceManager::MergeTaskResourceManager()
    : mIsDirty(false)
{
    mIsPack = EnvUtil::GetEnv("INDEXLIB_PACKAGE_MERGE_META", false);
}

MergeTaskResourceManager::~MergeTaskResourceManager() 
{
    Commit();
}

void MergeTaskResourceManager::Commit()
{
    if (!mIsDirty)
    {
        return;
    }
    assert(mRootDir);
    if (mIsPack)
    {
        mRootDir->GetFileSystem()->GetPackageStorage()->Commit();
    }
    mIsDirty = false;
}

IndexlibFileSystemPtr MergeTaskResourceManager::CreateFileSystem(const string& rootPath)
{
    string parentPath = PathUtil::GetParentDirPath(rootPath);
    string relativePath;
    PathUtil::GetRelativePath(parentPath, rootPath, relativePath);
    
    //prepare FileSystem
    IndexlibFileSystemPtr fs = IndexlibFileSystemCreator::Create(parentPath);
    return fs;
}

// init for write
void MergeTaskResourceManager::Init(const string& rootPath)
{
    string normPath = FileSystemWrapper::NormalizeDir(rootPath);
    if (FileSystemWrapper::IsExist(normPath))
    {
        IE_LOG(WARN, "path [%s] for merge resource data already exist, remove it!",
               normPath.c_str());
        FileSystemWrapper::DeleteDir(normPath);
    }
    FileSystemWrapper::MkDir(normPath, true);
    IndexlibFileSystemPtr fs = CreateFileSystem(normPath);
    mRootDir = DirectoryCreator::Get(fs, normPath, true);
    if (mIsPack)
    {
        fs->MountPackageStorage(normPath, "");
    }
}

// init for read
void MergeTaskResourceManager::Init(const MergeTaskResourceVec& resourceVec)
{
    string rootPath = "";
    for (resourceid_t id = 0; id < (resourceid_t)resourceVec.size(); id++)
    {
        auto resource = resourceVec[id];
        if (!resource || !resource->IsValid())
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "NULL or invalid MergeTaskResource in param!");
        }

        if (rootPath.empty())
        {
            rootPath = resource->GetRootPath();
            IndexlibFileSystemPtr fs = CreateFileSystem(rootPath);
            mRootDir = DirectoryCreator::Get(fs, rootPath, true);
            mRootDir->MountPackageFile(PACKAGE_FILE_PREFIX);
        }
        // copy merge task resource
        {
            ScopedLock lock(mLock);
            mResourceVec.push_back(resourceVec[id]);
            mResourceVec[id]->SetRoot(mRootDir);
        }
        if (resource->GetRootPath() != rootPath)
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "not same rootPath [%s:%s]!",
                    rootPath.c_str(), resource->GetRootPath().c_str());
        }
        if (resource->GetResourceId() != id)
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "resource id not match [%d:%d]!",
                    id, resource->GetResourceId());
        }
    }
}

const MergeTaskResourceManager::MergeTaskResourceVec&
MergeTaskResourceManager::GetResourceVec() const
{
    ScopedLock lock(mLock);
    return mResourceVec;
}

resourceid_t MergeTaskResourceManager::DeclareResource(
        const char* data, size_t dataLen, const string& description)
{
    if (!mRootDir)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "root path for merge resource should not be empty!");
    }
    MergeTaskResourcePtr resource;
    {
        ScopedLock lock(mLock);
        resource.reset(new MergeTaskResource(mRootDir, (resourceid_t)mResourceVec.size()));
        mResourceVec.push_back(resource);
        mIsDirty = true;
    }
    resource->Store(data, dataLen);
    resource->SetDescription(description);
    if (mIsPack)
    {
        mRootDir->GetFileSystem()->GetPackageStorage()->Sync();
    }
    return resource->GetResourceId();
}

MergeTaskResourcePtr MergeTaskResourceManager::GetResource(resourceid_t resId) const
{
    ScopedLock lock(mLock);
    if (resId < 0 || resId >= (resourceid_t)mResourceVec.size())
    {
        return MergeTaskResourcePtr();
    }
    
    MergeTaskResourcePtr resource =  mResourceVec[resId];
    if (resource && resource->GetResourceId() != resId)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "resource Id [%d:%d] not match!",
                             resId, resource->GetResourceId());
    }
    return resource;
}

const string& MergeTaskResourceManager::GetRootPath() const
{
    return mRootDir->GetPath();
}

IE_NAMESPACE_END(index_base);

