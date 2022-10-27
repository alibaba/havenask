#ifndef __INDEXLIB_MERGE_TASK_RESOURCE_H
#define __INDEXLIB_MERGE_TASK_RESOURCE_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index_base);

typedef int32_t resourceid_t;
#define INVALID_MERGE_RESOURCEID ((resourceid_t)-1)

class MergeTaskResource : public autil::legacy::Jsonizable
{
public:
    MergeTaskResource();
    MergeTaskResource(const MergeTaskResource& other);
    MergeTaskResource(const file_system::DirectoryPtr& rootDir, resourceid_t id);
    virtual ~MergeTaskResource();
    
public:
    /* store data to resource file */
    void Store(const char* data, size_t dataLen);

    /* fill buffer by reading data from resource file */
    size_t Read(char* buffer, size_t bufferSize);

    /* get inner data buffer, which life cycle hold by current resource object */
    char* GetData();
    
    size_t GetDataLen() const { return mDataLen; }
    const std::string& GetRootPath() const;
    void SetRoot(const file_system::DirectoryPtr& rootDir);
    
    resourceid_t GetResourceId() const { return mId; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    bool IsValid() const;

    void operator=(const MergeTaskResource& other);

    void SetDescription(const std::string& desc) { mDescription = desc; }
    const std::string& GetDescription() const { return mDescription; }
    
public:
    static std::string GetResourceFileName(resourceid_t resId);

private:
    void LoadInnerDataBuffer();
    
private:
    file_system::DirectoryPtr mRootDir;
    std::string mRootPath;
    resourceid_t mId;
    size_t mDataLen;
    char* mData;
    std::string mDescription;
    autil::ThreadMutex mLock;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTaskResource);

typedef std::vector<MergeTaskResourcePtr> MergeTaskResourceVector;

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_MERGE_TASK_RESOURCE_H
