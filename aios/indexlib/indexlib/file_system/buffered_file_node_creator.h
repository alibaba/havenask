#ifndef __INDEXLIB_BUFFERED_FILE_NODE_CREATOR_H
#define __INDEXLIB_BUFFERED_FILE_NODE_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node_creator.h"
#include "indexlib/file_system/session_file_cache.h"

IE_NAMESPACE_BEGIN(file_system);

class BufferedFileNodeCreator : public FileNodeCreator
{
public:
    BufferedFileNodeCreator(SessionFileCachePtr fileCache = SessionFileCachePtr());
    ~BufferedFileNodeCreator();

public:
    bool Init(const LoadConfig& loadConfig,
              const util::BlockMemoryQuotaControllerPtr& memController) override;
    FileNodePtr CreateFileNode(
            const std::string& filePath, FSOpenType type, bool readOnly) override;
    bool Match(const std::string& filePath, const std::string& lifecycle) const override;
    bool MatchType(FSOpenType type) const override;
    size_t EstimateFileLockMemoryUse(size_t fileLength) const override;

private:
    SessionFileCachePtr mFileCache;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BufferedFileNodeCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BUFFERED_FILE_NODE_CREATOR_H
