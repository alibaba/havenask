#ifndef __INDEXLIB_DEPLOY_INDEX_VALIDATOR_H
#define __INDEXLIB_DEPLOY_INDEX_VALIDATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);

IE_NAMESPACE_BEGIN(partition);

class DeployIndexValidator
{
public:
    DeployIndexValidator() {};
    ~DeployIndexValidator() {};
public:
    static void ValidateDeploySegments(const file_system::DirectoryPtr& rootDir,
                                       const index_base::Version& version);
private:    
    static void ValidateSingleSegmentDeployFiles(const file_system::DirectoryPtr& segDir);
    static void ValidatePartitionPatchSegmentDeployFiles(
            const file_system::DirectoryPtr& rootDir, const index_base::Version& version);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeployIndexValidator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_DEPLOY_INDEX_VALIDATOR_H
