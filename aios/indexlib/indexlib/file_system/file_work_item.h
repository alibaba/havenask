#ifndef __INDEXLIB_FS_FILEWORKITEM_H
#define __INDEXLIB_FS_FILEWORKITEM_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include <autil/WorkItem.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/buffered_file_output_stream.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/storage/file_buffer.h"

IE_NAMESPACE_BEGIN(file_system);

class WriteWorkItem : public autil::WorkItem 
{
public:
    WriteWorkItem(BufferedFileOutputStream* stream, storage::FileBuffer* fileBuffer);
    ~WriteWorkItem();
public:
    virtual void process();
    virtual void destroy();
    virtual void drop();
private:
    BufferedFileOutputStream* mStream;
    storage::FileBuffer* mFileBuffer;
};

class ReadWorkItem : public autil::WorkItem 
{
public:
    ReadWorkItem(FileNode* fileNode, storage::FileBuffer* fileBuffer, size_t offset,
        ReadOption option = ReadOption());
    ~ReadWorkItem();
public:
    virtual void process();
    virtual void destroy();
    virtual void drop();
private:
    FileNode* mFileNode;
    storage::FileBuffer *mFileBuffer;
    size_t mOffset;
    ReadOption mOption;
};

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FS_FILEWORKITEM_H
