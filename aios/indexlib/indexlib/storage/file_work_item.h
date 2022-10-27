#ifndef __INDEXLIB_FILEWORKITEM_H
#define __INDEXLIB_FILEWORKITEM_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include <autil/WorkItem.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/storage/file_buffer.h"
#include "indexlib/storage/file_wrapper.h"

IE_NAMESPACE_BEGIN(storage);

class WriteWorkItem : public autil::WorkItem 
{
public:
    WriteWorkItem(fslib::fs::File *file, FileBuffer *fileBuffer);
    ~WriteWorkItem();
public:
    virtual void process();
    virtual void destroy();
    virtual void drop();
private:
    fslib::fs::File *mFile;
    FileBuffer *mFileBuffer;
};

class ReadWorkItem : public autil::WorkItem 
{
public:
    ReadWorkItem(fslib::fs::File *file, FileBuffer *fileBuffer);
    ~ReadWorkItem();
public:
    virtual void process();
    virtual void destroy();
    virtual void drop();
private:
    fslib::fs::File *mFile;
    FileBuffer *mFileBuffer;
};

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_FILEWORKITEM_H
