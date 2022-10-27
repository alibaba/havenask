#ifndef __INDEXLIB_SINGLE_FILE_FLUSH_OPERATION_H
#define __INDEXLIB_SINGLE_FILE_FLUSH_OPERATION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/file_flush_operation.h"

IE_NAMESPACE_BEGIN(file_system);

class SingleFileFlushOperation : public FileFlushOperation
{
public:
    SingleFileFlushOperation(const FileNodePtr& fileNode,
                             const FSDumpParam& param = FSDumpParam());
    ~SingleFileFlushOperation();

public:
    void Execute(const PathMetaContainerPtr& pathMetaContainer) override;
    const std::string& GetPath() const override
    { return mDestPath; }
    void GetFileNodePaths(fslib::FileList& fileList) const override;
    
private:
    void AtomicStore(const std::string& filePath);
    void Store(const std::string& filePath);

private:
    std::string mDestPath;
    FileNodePtr mFileNode;
    FileNodePtr mFlushFileNode;
    FSDumpParam mDumpParam;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleFileFlushOperation);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_SINGLE_FILE_FLUSH_OPERATION_H
