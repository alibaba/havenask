#ifndef __INDEXLIB_FILE_NODE_CACHE_H
#define __INDEXLIB_FILE_NODE_CACHE_H

#include <tr1/memory>
#include <functional>
#include <fslib/fslib.h>
#include <autil/Lock.h>
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/storage_metrics.h"
#include "indexlib/file_system/directory_map_iterator.h"
#include "indexlib/util/path_util.h"

IE_NAMESPACE_BEGIN(file_system);

// ASSUME: directory(in mem only)  always is dirty
class FileNodeCache 
{
public:
    typedef std::tr1::unordered_map<std::string, FileNodePtr*> FastFileNodeMap;
    
    typedef DirectoryMapIterator<FileNodePtr> DirectoryIterator;
    typedef DirectoryIterator::DirectoryMap FileNodeMap;
    typedef FileNodeMap::iterator Iterator;
    typedef FileNodeMap::const_iterator ConstIterator;
    
public:
    FileNodeCache(StorageMetrics* metrics);
    ~FileNodeCache();

public:
    void Insert(const FileNodePtr& fileNode);
    bool RemoveFile(const std::string& filePath);
    bool RemoveDirectory(const std::string& dirPath);
    bool IsExist(const std::string& path) const;
    // physical=true: list on disk files only
    void ListFile(const std::string& dirPath, fslib::FileList& fileList,
                  bool recursive = false, bool physical = false) const;
    FileNodePtr Find(const std::string& path) const;
    void Truncate(const std::string& filePath, size_t newLength);
    void Clean();
    void CleanFiles(const fslib::FileList& fileList);
    
    size_t GetFileCount()
    { return mFileNodeMap.size(); }
    uint64_t GetUseCount(const std::string& filePath) const;

public:
    autil::RecursiveThreadMutex* GetCacheLock()
    { return &mLock; }
    
    ConstIterator Begin() const { return mFileNodeMap.begin(); }
    ConstIterator End() const { return mFileNodeMap.end(); }

private:
    bool DoIsExist(const std::string& path) const;
    bool CheckRemoveDirectoryPath(const std::string& normalPath) const;
    void IncreaseMetrics(FSFileType type, size_t length)
    { if (mMetrics) { mMetrics->IncreaseFile(type, length); } }
    void DecreaseMetrics(FSFileType type, size_t length)
    { if (mMetrics) { mMetrics->DecreaseFile(type, length); } }
    void IncreaseRemoveMetrics(FSFileType type);
    void RemoveFileNode(const FileNodePtr& fileNode);
private:
    mutable autil::RecursiveThreadMutex mLock;
    mutable FileNodeMap mFileNodeMap;
    mutable FastFileNodeMap mFastFileNodeMap;
    StorageMetrics* mMetrics;

private:
    IE_LOG_DECLARE();
    friend class FileNodeCacheTest;
};

DEFINE_SHARED_PTR(FileNodeCache);

////////////////////////////////////////////////////////////
inline void FileNodeCache::IncreaseRemoveMetrics(FSFileType type)
{
    if (mMetrics && type != FSFT_DIRECTORY)
    {
        mMetrics->IncreaseFileCacheRemove();
    }
}

inline bool FileNodeCache::IsExist(const std::string& path) const
{
    std::string normalPath = util::PathUtil::NormalizePath(path);
    //const std::string& normalPath = path;
    autil::ScopedLock lock(mLock);
    return DoIsExist(normalPath);
}

inline bool FileNodeCache::DoIsExist(const std::string& path) const
{
    return mFastFileNodeMap.find(path) != mFastFileNodeMap.end();
}

inline FileNodePtr FileNodeCache::Find(const std::string& path) const
{
    autil::ScopedLock lock(mLock);
    FastFileNodeMap::const_iterator it = mFastFileNodeMap.find(path);
    if (it == mFastFileNodeMap.end())
    {
        return FileNodePtr();
    }
    return *(it->second);
}

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_NODE_CACHE_H
