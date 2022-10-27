#include "indexlib/file_system/path_meta_container.h"
#include "indexlib/file_system/directory_map_iterator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
using namespace fslib;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, PathMetaContainer);

PathMetaContainer::PathMetaContainer() 
{
}

PathMetaContainer::~PathMetaContainer() 
{
}

bool PathMetaContainer::AddFileInfo(
        const string& filePath, int64_t fileLength,
        uint64_t modifyTime, bool isDirectory)
{
    string normPath = PathUtil::NormalizePath(filePath);
    string path = isDirectory ? normPath + "/" : normPath;
    if (!isDirectory && fileLength < 0)
    {
        fileLength = FileSystemWrapper::GetFileLength(path, false);
    }
    FileInfoPtr info(new FileInfo(path, fileLength, modifyTime));

    ScopedLock lock(mLock);
    FastFileInfoMap::const_iterator iter = mFastMap.find(normPath);
    if (iter != mFastMap.end())
    {
        if (iter->second->filePath != path)
        {
            IE_LOG(ERROR, "conflict filePath [%s:%s]!",
                   iter->second->filePath.c_str(), path.c_str());
            return false;
        }

        if (iter->second->fileLength != fileLength)
        {
            IE_LOG(ERROR, "conflict fileLength [%ld:%ld]!", iter->second->fileLength, fileLength);
            return false;
        }
        
        if (iter->second->modifyTime == (uint64_t)-1 ||
            (modifyTime != (uint64_t)-1 && iter->second->modifyTime < modifyTime))
        {
            iter->second->modifyTime = modifyTime;
        }
        return true;
    }
    mFileInfoMap[normPath] = info;
    mFastMap[normPath] = info;
    return true;
}

void PathMetaContainer::MarkNotExistSolidPath(const string& solidPath)
{
    string normSolidPath = PathUtil::NormalizePath(solidPath);
    ScopedLock lock(mLock);
    mSolidPathMap[normSolidPath] = false;
}

bool PathMetaContainer::AddFileInfoForOneSolidPath(const string& solidPath,
        vector<FileInfo>::const_iterator firstFileInfo,
        vector<FileInfo>::const_iterator lastFileInfo)
{
    string normSolidPath = PathUtil::NormalizePath(solidPath);
    ScopedLock lock(mLock);
    for (auto it = firstFileInfo; it != lastFileInfo; it ++)
    {
        string filePath = PathUtil::NormalizePath(
                PathUtil::JoinPath(normSolidPath, it->filePath));
        if (!AddFileInfo(filePath, it->fileLength, it->modifyTime, it->isDirectory()))
        {
            IE_LOG(WARN, "add path meta to container for filePath [%s] fail!",
                   filePath.c_str());
            return false;
        }
    }
    mSolidPathMap.insert(make_pair(normSolidPath, true));
    return true;
}

bool PathMetaContainer::RemoveFile(const string& filePath)
{
    string normalPath = PathUtil::NormalizePath(filePath);
    ScopedLock lock(mLock);
    FileInfoMap::const_iterator it = mFileInfoMap.find(normalPath);
    if (it != mFileInfoMap.end())
    {
        const FileInfoPtr& fileInfo = it->second;
        if (fileInfo->isDirectory())
        {
            IE_LOG(ERROR, "Path [%s] is a directory, remove failed",
                   normalPath.c_str());  
            return false;
        }
        mFastMap.erase(normalPath); // fastFileNodeMap should erase first
        mFileInfoMap.erase(normalPath);
        return true;
    }
    return false;
}
    
bool PathMetaContainer::RemoveDirectory(const string& path)
{
    string normalPath = PathUtil::NormalizePath(path);
    ScopedLock lock(mLock);
    FileInfoPtr infoPtr;
    FastFileInfoMap::const_iterator iter = mFastMap.find(normalPath);
    if (iter != mFastMap.end())
    {
        infoPtr = iter->second;
    }
    if (infoPtr && !infoPtr->isDirectory())
    {
        IE_LOG(ERROR, "path [%s] is not directory!", normalPath.c_str());
        return false;
    }
    DirectoryMapIterator<FileInfoPtr> iterator(mFileInfoMap, normalPath);
    while (iterator.HasNext())
    {
        auto curIt = iterator.Next();
        mFastMap.erase(curIt->first);
        iterator.Erase(curIt);
    }

    FileInfoMap::iterator it = mFileInfoMap.find(normalPath);
    if (it != mFileInfoMap.end())
    {
        mFastMap.erase(it->first);
        mFileInfoMap.erase(it);
    }

    SolidPathMap::const_iterator pIter = mSolidPathMap.begin();
    for ( ; pIter != mSolidPathMap.end(); )
    {
        const string& solidPath = pIter->first;
        if (solidPath == normalPath || PathUtil::IsInRootPath(solidPath, normalPath))
        {
            mSolidPathMap.erase(pIter++);
            continue;
        }
        pIter++;
    }
    return true;
}

bool PathMetaContainer::MatchSolidPath(const string& path) const
{
    string normalPath = PathUtil::NormalizePath(path);
    ScopedLock lock(mLock);
    SolidPathMap::const_iterator iter = mSolidPathMap.begin();
    for ( ; iter != mSolidPathMap.end(); iter++)
    {
        const string& solidPath = iter->first;
        if (PathUtil::IsInRootPath(normalPath, solidPath))
        {
            return true;
        }
    }
    return false;
}

bool PathMetaContainer::IsExist(const string& path) const
{
    string normalPath = PathUtil::NormalizePath(path);
    ScopedLock lock(mLock);
    if (mFastMap.find(normalPath) != mFastMap.end())
    {
        return true;
    }
    auto iter = mSolidPathMap.find(normalPath);
    return iter != mSolidPathMap.end() && iter->second;
}

bool PathMetaContainer::ListFile(const string& dirPath,
                                 FileList& fileList, bool recursive)
{
    fileList.clear();
    string normalPath = PathUtil::NormalizePath(dirPath);
    ScopedLock lock(mLock);
    FileInfoPtr infoPtr;
    FastFileInfoMap::const_iterator iter = mFastMap.find(normalPath);
    if (iter != mFastMap.end())
    {
        infoPtr = iter->second;
    }

    if (infoPtr && !infoPtr->isDirectory())
    {
        IE_LOG(ERROR, "path [%s] is not directory!", normalPath.c_str());
        return false;
    }
    DirectoryMapIterator<FileInfoPtr> iterator(mFileInfoMap, normalPath);
    while (iterator.HasNext())
    {
        auto curIt = iterator.Next();
        const string& path = curIt->first;
        if (!recursive && path.find("/", normalPath.size() + 1) != string::npos)
        {
            continue;
        }

        string relativePath = path.substr(normalPath.size() + 1);
        if (recursive && curIt->second->isDirectory())
        {
            relativePath += "/";
        }
        fileList.push_back(relativePath);
    }
    return true;
}

bool PathMetaContainer::GetFileInfo(const string& path, FileInfo& fileInfo) const
{
    string normalPath = PathUtil::NormalizePath(path);
    ScopedLock lock(mLock);
    FastFileInfoMap::const_iterator iter = mFastMap.find(normalPath);
    if (iter != mFastMap.end())
    {
        fileInfo = *(iter->second);
        return true;
    }

    auto pIter = mSolidPathMap.find(normalPath);
    if (pIter != mSolidPathMap.end() && pIter->second)
    {
        fileInfo = FileInfo(pIter->first + "/", -1, (uint64_t)-1);
        return true;
    }
    return false;
}

size_t PathMetaContainer::GetFileLength(const string& filePath) const
{
    string normalPath = PathUtil::NormalizePath(filePath);
    ScopedLock lock(mLock);
    FastFileInfoMap::const_iterator iter = mFastMap.find(normalPath);
    if (iter == mFastMap.end())
    {
        return 0;
    }
    return iter->second->fileLength;
}

IE_NAMESPACE_END(file_system);

