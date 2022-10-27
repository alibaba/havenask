#ifndef __INDEXLIB_DIRECTORY_FILE_NODE_CREATOR_H
#define __INDEXLIB_DIRECTORY_FILE_NODE_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_node_creator.h"

IE_NAMESPACE_BEGIN(file_system);

class DirectoryFileNodeCreator : public FileNodeCreator
{
public:
    DirectoryFileNodeCreator();
    ~DirectoryFileNodeCreator();

public:
    bool Init(const LoadConfig& loadConfig,
                             const util::BlockMemoryQuotaControllerPtr& memController) override;
    FileNodePtr CreateFileNode(const std::string& filePath,
                               FSOpenType type, bool readOnly) override;
    bool Match(const std::string& filePath, const std::string& lifecycle) const override;
    bool MatchType(FSOpenType type) const override;
    size_t EstimateFileLockMemoryUse(size_t fileLength) const override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DirectoryFileNodeCreator);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DIRECTORY_FILE_NODE_CREATOR_H
