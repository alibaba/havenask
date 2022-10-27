#ifndef __INDEXLIB_FILE_FLUSH_OPERATION_H
#define __INDEXLIB_FILE_FLUSH_OPERATION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/path_meta_container.h"
#include "indexlib/storage/file_wrapper.h"
#include "fslib/common/common_type.h"

IE_NAMESPACE_BEGIN(file_system);

class FileFlushOperation
{
public:
    FileFlushOperation():mFlushMemoryUse(0) {}
    virtual ~FileFlushOperation() {}
    int64_t GetFlushMemoryUse() const { return mFlushMemoryUse; }

public:
    virtual void Execute(const PathMetaContainerPtr& pathMetaContainer) = 0;
    virtual const std::string& GetPath() const = 0;
    virtual void GetFileNodePaths(fslib::FileList& fileList) const = 0;

protected:
    static const uint32_t DEFAULT_FLUSH_BUF_SIZE = 2 * 1024 * 1024; // 2 MB

    void SplitWrite(const storage::FileWrapperPtr& file,
                    const void* buffer, size_t length)
    {
        size_t leftToWriteLen = length;
        size_t totalWriteLen = 0;
        while (leftToWriteLen > 0) 
        {
            size_t writeLen = (leftToWriteLen > DEFAULT_FLUSH_BUF_SIZE) ?
                              DEFAULT_FLUSH_BUF_SIZE : leftToWriteLen;
            file->Write((uint8_t*)buffer + totalWriteLen, writeLen);
            leftToWriteLen -= writeLen;
            totalWriteLen += writeLen;
        }
    }
    
protected:
    int64_t mFlushMemoryUse;
    
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileFlushOperation);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_FLUSH_OPERATION_H
