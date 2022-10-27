#ifndef __INDEXLIB_MERGE_TASK_RESOURCE_MANAGER_H
#define __INDEXLIB_MERGE_TASK_RESOURCE_MANAGER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);

IE_NAMESPACE_BEGIN(index_base);

class MergeTaskResourceManager
{
public:
    typedef std::vector<MergeTaskResourcePtr> MergeTaskResourceVec;
    
public:
    MergeTaskResourceManager();
    ~MergeTaskResourceManager();
    
public:
    // init for declare from empty root
    void Init(const std::string& rootPath);

    // init from resource vector
    void Init(const MergeTaskResourceVec& resourceVec);

    // create new merge resource, return resource id
    resourceid_t DeclareResource(const char* data, size_t dataLen,
                                 const std::string& description = "");

    // get resource by resource id
    MergeTaskResourcePtr GetResource(resourceid_t resId) const;
    
    const MergeTaskResourceVec& GetResourceVec() const;

    const std::string& GetRootPath() const;
    
    void Commit();
        
private:
    static file_system::IndexlibFileSystemPtr CreateFileSystem(const std::string& rootPath);
private:
    MergeTaskResourceVec mResourceVec;
    file_system::DirectoryPtr mRootDir;
    bool mIsPack;
    bool mIsDirty;
    mutable autil::ThreadMutex mLock;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTaskResourceManager);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_MERGE_TASK_RESOURCE_MANAGER_H
