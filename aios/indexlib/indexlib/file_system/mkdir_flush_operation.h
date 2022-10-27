#ifndef __INDEXLIB_MKDIR_FLUSH_OPERATION_H
#define __INDEXLIB_MKDIR_FLUSH_OPERATION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class MkdirFlushOperation
{
public:
    MkdirFlushOperation(const FileNodePtr& fileNode);
    ~MkdirFlushOperation();

public:
    void Execute();
    const std::string& GetPath()
    { return mDestPath; }

private:
    std::string mDestPath;
    FileNodePtr mFileNode;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MkdirFlushOperation);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MKDIR_FLUSH_OPERATION_H
