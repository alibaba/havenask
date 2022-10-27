#ifndef __INDEXLIB_FILE_NODE_CREATOR_H
#define __INDEXLIB_FILE_NODE_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/package_file_mount_table.h"
#include "indexlib/file_system/load_config/load_config.h"

IE_NAMESPACE_BEGIN(file_system);

class FileNodeCreator
{
public:
    FileNodeCreator();
    virtual ~FileNodeCreator();

public:
    virtual bool Init(const LoadConfig& loadConfig,
                      const util::BlockMemoryQuotaControllerPtr& memController) = 0;
    virtual FileNodePtr CreateFileNode(const std::string& filePath,
            FSOpenType type, bool readOnly) = 0;

    virtual bool Match(const std::string& filePath, const std::string& lifecycle) const = 0;
    virtual bool MatchType(FSOpenType type) const = 0;
    virtual bool IsRemote() const
    { return false; }

    // just FSOT_LOAD_CONFIG used
    virtual FSOpenType GetDefaultOpenType() const
    { assert(false); return FSOT_UNKNOWN; }
    virtual size_t EstimateFileLockMemoryUse(size_t fileLength) const =  0;
    virtual bool OnlyPatialLock() const { return false; }
protected:
    util::BlockMemoryQuotaControllerPtr mMemController;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileNodeCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILE_NODE_CREATOR_H
