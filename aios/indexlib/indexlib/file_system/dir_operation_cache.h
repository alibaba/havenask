#ifndef __INDEXLIB_DIROPERATIONCACHE_H
#define __INDEXLIB_DIROPERATIONCACHE_H

#include <tr1/memory>
#include <set>
#include <autil/Lock.h>
#include "indexlib/common_define.h"
#include "indexlib/file_system/path_meta_container.h"

IE_NAMESPACE_BEGIN(file_system);

class DirOperationCache
{
public:
    DirOperationCache(const PathMetaContainerPtr& pathMetaContainer = PathMetaContainerPtr());
    ~DirOperationCache();
public:
    void MkParentDirIfNecessary(const std::string &path);
    // we will normalize path inside
    void Mkdir(const std::string &path);
private:
    bool Get(const std::string &path);
    void Set(const std::string &path);
public:
    // path has to be a file!
    static std::string GetParent(const std::string &path);
private:
    PathMetaContainerPtr mPathMetaContainer;
    autil::ThreadMutex mDirSetMutex;
    autil::ThreadMutex mMkDirMutex;
    std::set<std::string> mDirs;
private:
    IE_LOG_DECLARE();
    friend class DirOperationCacheTest;
};

DEFINE_SHARED_PTR(DirOperationCache);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DIROPERATIONCACHE_H
