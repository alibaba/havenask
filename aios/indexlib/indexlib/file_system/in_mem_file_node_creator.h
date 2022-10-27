#ifndef __INDEXLIB_IN_MEM_FILE_NODE_CREATOR_H
#define __INDEXLIB_IN_MEM_FILE_NODE_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node_creator.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/session_file_cache.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemFileNodeCreator : public FileNodeCreator
{
public:
    InMemFileNodeCreator(SessionFileCachePtr fileCache = SessionFileCachePtr());
    ~InMemFileNodeCreator();

public:
    bool Init(const LoadConfig& loadConfig,
              const util::BlockMemoryQuotaControllerPtr& memController) override;
    FileNodePtr CreateFileNode(
        const std::string& filePath, FSOpenType type, bool readOnly) override;
    bool Match(const std::string& filePath, const std::string& lifecycle) const override;
    bool MatchType(FSOpenType type) const override;
    bool IsRemote() const override;
    FSOpenType GetDefaultOpenType() const override { return FSOT_IN_MEM; }
    size_t EstimateFileLockMemoryUse(size_t fileLength) const override;

public:
    // for test
    static InMemFileNode* Create()
    {
        util::BlockMemoryQuotaControllerPtr controller =
            util::MemoryQuotaControllerCreator::CreateBlockMemoryController();
        return new InMemFileNode(0, true, LoadConfig(), controller);
    }
    static InMemFileNode* Create(const std::string& path)
    {
        InMemFileNode* fileNode = Create();
        fileNode->Open(path, file_system::FSOT_IN_MEM);
        fileNode->Populate();
        return fileNode;
    }
private:
    SessionFileCachePtr mFileCache;
    LoadConfig mLoadConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemFileNodeCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_IN_MEM_FILE_NODE_CREATOR_H
