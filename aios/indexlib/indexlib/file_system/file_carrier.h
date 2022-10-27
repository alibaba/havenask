#ifndef __INDEXLIB_FILE_CARRIER_H
#define __INDEXLIB_FILE_CARRIER_H

#include <tr1/memory>
#include <fslib/fs/FileSystem.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, MmapFileNode);

IE_NAMESPACE_BEGIN(file_system);

class FileCarrier
{
public:
    FileCarrier() = default;
    ~FileCarrier() = default;
public:
    MmapFileNodePtr GetMmapLockFileNode() { return mMmapLockFile; }
    void SetMmapLockFileNode(const MmapFileNodePtr& fileNode)
        { assert(fileNode); mMmapLockFile = fileNode; }
    inline long GetMmapLockFileNodeUseCount() const { return mMmapLockFile.use_count(); }

private:
    MmapFileNodePtr mMmapLockFile;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileCarrier);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_CARRIER_H
