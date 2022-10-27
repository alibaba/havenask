#include "indexlib/file_system/dir_operation_cache.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <autil/Lock.h>
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, DirOperationCache);

DirOperationCache::DirOperationCache(const PathMetaContainerPtr& pathMetaContainer)
    : mPathMetaContainer(pathMetaContainer)
{
}

DirOperationCache::~DirOperationCache()
{
}

void DirOperationCache::MkParentDirIfNecessary(const string &path) {
    string parent = GetParent(path);
    if (FileSystemWrapper::NeedMkParentDirBeforeOpen(path))
    {
        Mkdir(parent);
    }
    else
    {
        Set(parent);
    }
}

void DirOperationCache::Mkdir(const string &path) {
    if (!Get(path))
    {
        ScopedLock lock(mMkDirMutex);
        if (!Get(path))
        {
            FileSystemWrapper::MkDirIfNotExist(path);
            uint64_t time = TimeUtility::currentTimeInSeconds();
            if (mPathMetaContainer)
            {
                mPathMetaContainer->AddFileInfo(path, -1, time, true);
                mPathMetaContainer->AddFileInfo(GetParent(path), -1, time, true);
            }
            Set(path);
        }
    }
}

bool DirOperationCache::Get(const string &path)
{
    string normalizedPath = FileSystemWrapper::NormalizeDir(path);
    ScopedLock lock(mDirSetMutex);
    return mDirs.find(normalizedPath) != mDirs.end();
}

void DirOperationCache::Set(const string &path)
{
    string normalizedPath = FileSystemWrapper::NormalizeDir(path);
    ScopedLock lock(mDirSetMutex);
    mDirs.insert(normalizedPath);
}

string DirOperationCache::GetParent(const string &path)
{
    vector<string> dirs = StringUtil::split(path, "/", false);
    dirs.pop_back();
    return StringUtil::toString(dirs, "/");
}

IE_NAMESPACE_END(file_system);
