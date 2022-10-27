#ifndef __INDEXLIB_MMAP_FILE_NODE_CREATOR_H
#define __INDEXLIB_MMAP_FILE_NODE_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/file_node_creator.h"

IE_NAMESPACE_BEGIN(file_system);

class MmapFileNodeCreator : public FileNodeCreator
{
public:
    MmapFileNodeCreator();
    ~MmapFileNodeCreator();

public:
    bool Init(const LoadConfig& loadConfig,
              const util::BlockMemoryQuotaControllerPtr& memController) override;
    FileNodePtr CreateFileNode(
            const std::string& filePath, FSOpenType type, bool readOnly) override;
    bool Match(const std::string& filePath, const std::string& lifecycle) const override;
    bool MatchType(FSOpenType type) const override;
    bool IsRemote() const override;
    FSOpenType GetDefaultOpenType() const override { return FSOT_MMAP; }
    size_t EstimateFileLockMemoryUse(size_t fileLength) const override;
    bool OnlyPatialLock() const override;
    
public:
    bool IsLock() const;

private:
    LoadConfig mLoadConfig;
    bool mLock;
    bool mFullMemory;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MmapFileNodeCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MMAP_FILE_NODE_CREATOR_H
