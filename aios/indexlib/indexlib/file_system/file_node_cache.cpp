#include "indexlib/file_system/file_node_cache.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileNodeCache);

FileNodeCache::FileNodeCache(StorageMetrics* metrics)
    : mMetrics(metrics)
{
}

FileNodeCache::~FileNodeCache() 
{
}

void FileNodeCache::Insert(const FileNodePtr& fileNode)
{
    string normalPath = fileNode->GetPath();
    
    ScopedLock lock(mLock);

    FileNodeMap::const_iterator it = mFileNodeMap.find(normalPath);
    if (it != mFileNodeMap.end())
    {
        assert(it->second.use_count() == 1);
        const FileNodePtr& cacheFileNode = it->second;
        DecreaseMetrics(cacheFileNode->GetType(), cacheFileNode->GetLength());
    }

    // replace when exist
    mFileNodeMap[normalPath] = fileNode;
    FileNodeMap::iterator iter = mFileNodeMap.find(normalPath);
    FileNodePtr& inMapNode = iter->second;
    mFastFileNodeMap[normalPath] = &inMapNode;
    IncreaseMetrics(fileNode->GetType(), fileNode->GetLength());
}

bool FileNodeCache::RemoveFile(const string& filePath)
{
    assert(!filePath.empty());
    assert(*(filePath.rbegin()) != '/');
    string normalPath = PathUtil::NormalizePath(filePath);

    ScopedLock lock(mLock);

    FileNodeMap::const_iterator it = mFileNodeMap.find(normalPath);
    if (it != mFileNodeMap.end())
    {
        const FileNodePtr& fileNode = it->second;

        if (fileNode->GetType() == FSFT_DIRECTORY)
        {
            IE_LOG(ERROR, "Path [%s] is a directory, remove failed",
                   normalPath.c_str());  
            return false;
        }

        if (fileNode.use_count() > 1)
        {
            IE_LOG(ERROR, "File [%s] remove fail, because the use count[%ld] >1",
                   normalPath.c_str(), fileNode.use_count());  
            return false;
        }
        RemoveFileNode(fileNode);
        mFastFileNodeMap.erase(normalPath); // fastFileNodeMap should erase first
        mFileNodeMap.erase(normalPath);
        return true;
    }

    IE_LOG(DEBUG, "File [%s] not in cache,", normalPath.c_str());
    return false;
}

bool FileNodeCache::RemoveDirectory(const std::string& dirPath)
{
    string normalPath = PathUtil::NormalizePath(dirPath);

    ScopedLock lock(mLock);
    if (!CheckRemoveDirectoryPath(normalPath))
    {
        return false;
    }

    DirectoryIterator iterator(mFileNodeMap, normalPath);
    // eg.: inmem, inmem/1, inmem/2, rm inmem
    // rm : inmem/1, inmem/2
    while (iterator.HasNext())
    {
        Iterator curIt = iterator.Next();
        const FileNodePtr& fileNode = curIt->second;            
        RemoveFileNode(fileNode);
        mFastFileNodeMap.erase(fileNode->GetPath());
        iterator.Erase(curIt);
    }

    // rm : inmem
    FileNodeMap::iterator it = mFileNodeMap.find(normalPath);
    if (it != mFileNodeMap.end())
    {
        RemoveFileNode(it->second);
        mFastFileNodeMap.erase(it->second->GetPath());
        mFileNodeMap.erase(it);
    }

    IE_LOG(TRACE1, "Directory [%s] removed", dirPath.c_str());
    return true;
}

void FileNodeCache::ListFile(const string& dirPath, FileList& fileList,
                             bool recursive, bool physical) const
{
    string normalPath = PathUtil::NormalizePath(dirPath);

    ScopedLock lock(mLock);
    DirectoryIterator iterator(mFileNodeMap, normalPath);
    while (iterator.HasNext())
    {
        Iterator it = iterator.Next();
        const string& path = it->first;
        if (!recursive && path.find("/", normalPath.size() + 1) != string::npos)
        {
            continue;
        }
        if (physical && it->second->IsInPackage())
        {
            continue;
        }
        string relativePath = path.substr(normalPath.size() + 1);
        if (recursive && it->second->GetType() == FSFT_DIRECTORY)
        {
            // for index deploy
            relativePath += "/";
        }
        fileList.push_back(relativePath);
    }
}

void FileNodeCache::Truncate(const string& filePath, size_t newLength)
{
    string normalPath = PathUtil::NormalizePath(filePath);

    ScopedLock lock(mLock);

    FileNodeMap::const_iterator it = mFileNodeMap.find(normalPath);
    if (it != mFileNodeMap.end())
    {
        const FileNodePtr& cacheFileNode = it->second;
        DecreaseMetrics(cacheFileNode->GetType(), cacheFileNode->GetLength());
        IncreaseMetrics(cacheFileNode->GetType(), newLength);
    }
}

void FileNodeCache::Clean()
{
    ScopedLock lock(mLock);

    for(FileNodeMap::iterator it = mFileNodeMap.begin(); 
        it != mFileNodeMap.end();)
    {
        const FileNodePtr& fileNode = it->second;
        // IsDirty for no sync
        if (fileNode->IsDirty()
            || fileNode.use_count() > 1)
        {
            ++it;
            continue;
        }
        RemoveFileNode(fileNode);
        mFastFileNodeMap.erase(fileNode->GetPath());
        mFileNodeMap.erase(it++);
    }
    IE_LOG(TRACE1, "Cleaned");
}

void FileNodeCache::CleanFiles(const FileList& fileList)
{
    ScopedLock lock(mLock);

    for (size_t i = 0; i < fileList.size(); i++)
    {
        const FileNodeMap::iterator it = mFileNodeMap.find(fileList[i]);
        if (it != mFileNodeMap.end())
        {
            const FileNodePtr& fileNode = it->second;
            if (!fileNode->IsDirty()
                && fileNode.use_count() == 1)
            {
                RemoveFileNode(fileNode);
                mFastFileNodeMap.erase(fileNode->GetPath());
                mFileNodeMap.erase(it);
            }
        }
    }
    IE_LOG(TRACE1, "Cleaned");
}

bool FileNodeCache::CheckRemoveDirectoryPath(const string& normalPath) const
{
    FileNodeMap::const_iterator it = mFileNodeMap.find(normalPath);
    if (it != mFileNodeMap.end())
    {
        FileNodePtr fileNode = it->second;
        if (fileNode->GetType() != FSFT_DIRECTORY)
        {
            IE_LOG(ERROR, "Path [%s] is a file, remove failed",
                   normalPath.c_str());
            return false;
        }
    }
    else
    {
        IE_LOG(DEBUG, "Directory [%s] does not exist", normalPath.c_str());
    }

    DirectoryIterator iterator(mFileNodeMap, normalPath);
    while (iterator.HasNext())
    {
        Iterator it = iterator.Next();
        const FileNodePtr& fileNode = it->second;
        if (fileNode.use_count() > 1)
        {
            IE_LOG(ERROR, "File [%s] in directory [%s] remove fail,"
                   "because the use count [%ld]>1", normalPath.c_str(), 
                   fileNode->GetPath().c_str(), fileNode.use_count());  
            return false;
        }
    }

    return true;
}

void FileNodeCache::RemoveFileNode(const FileNodePtr& fileNode)
{
    IE_LOG(TRACE1, "Remove file [%s], type [%d] len [%lu]",
           fileNode->GetPath().c_str(), fileNode->GetType(),
           fileNode->GetLength());
    DecreaseMetrics(fileNode->GetType(), fileNode->GetLength());
    IncreaseRemoveMetrics(fileNode->GetType());
}

uint64_t FileNodeCache::GetUseCount(const string& filePath) const
{
    autil::ScopedLock lock(mLock);
    FastFileNodeMap::const_iterator it = mFastFileNodeMap.find(filePath);
    if (it == mFastFileNodeMap.end())
    {
        return 0;
    }
    return (it->second)->use_count();
}

IE_NAMESPACE_END(file_system);

