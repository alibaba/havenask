#ifndef ISEARCH_BS_MERGERESOURCEPROVIDER_H
#define ISEARCH_BS_MERGERESOURCEPROVIDER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/custom_merger/TaskItemDispatcher.h"
#include <indexlib/partition/remote_access/partition_resource_provider.h>

namespace build_service {
namespace custom_merger {

class MergeResourceProvider
{
public:
    MergeResourceProvider();
    virtual ~MergeResourceProvider();
private:
    MergeResourceProvider(const MergeResourceProvider &);
    MergeResourceProvider& operator=(const MergeResourceProvider &);
public:
    bool init(const std::string& indexPath,
              const IE_NAMESPACE(config)::IndexPartitionOptions& options,
              const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& newSchema,
              const std::string& pluginPath,
              int32_t targetVersionId,
              int32_t checkpointVersionId);

    typedef TaskItemDispatcher::SegmentInfo SegmentInfo;
    IE_NAMESPACE(index_base)::Version getVersion() {
        return _indexProvider->GetVersion();
    }
    virtual void getIndexSegmentInfos(std::vector<SegmentInfo>& segmentInfos);
    
    virtual IE_NAMESPACE(config)::IndexPartitionSchemaPtr getNewSchema()
    {
        return _newSchema;
    }

    virtual IE_NAMESPACE(config)::IndexPartitionSchemaPtr getOldSchema()
    {
        if (!_newSchema->HasModifyOperations()) {
            return _indexProvider->GetSchema();
        }
        IE_NAMESPACE(config)::IndexPartitionSchemaPtr oldSchema;
        uint32_t operationCount = _newSchema->GetModifyOperationCount();
        if (operationCount == 1) {
            oldSchema.reset(_newSchema->CreateSchemaWithoutModifyOperations());
        } else {
            oldSchema.reset(_newSchema->CreateSchemaForTargetModifyOperation(
                            operationCount - 1));
        }
        return oldSchema;
    }
    
    virtual IE_NAMESPACE(partition)::PartitionResourceProviderPtr getPartitionResourceProvider()
    {
        return _indexProvider;
    }

protected:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _newSchema;
    IE_NAMESPACE(partition)::PartitionResourceProviderPtr _indexProvider;
    int32_t _checkpointVersionId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MergeResourceProvider);

}
}

#endif //ISEARCH_BS_MERGERESOURCEPROVIDER_H
