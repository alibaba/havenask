#ifndef ISEARCH_BS_ALTERDEFAULTFIELDMERGER_H
#define ISEARCH_BS_ALTERDEFAULTFIELDMERGER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/custom_merger/CustomMergerImpl.h"
#include "build_service/custom_merger/CustomMergerTaskItem.h"
#include <indexlib/partition/remote_access/partition_resource_provider.h>

namespace build_service {
namespace custom_merger {

class AlterDefaultFieldMerger : public CustomMergerImpl
{
public:
    AlterDefaultFieldMerger();
    ~AlterDefaultFieldMerger();
private:
    AlterDefaultFieldMerger(const AlterDefaultFieldMerger &);
    AlterDefaultFieldMerger& operator=(const AlterDefaultFieldMerger &);

public:
    bool estimateCost(std::vector<CustomMergerTaskItem>& taskItem) override;

protected:
    bool doMergeTask(const CustomMergePlan::TaskItem& taskItem,
                     const std::string& dir) override;

    virtual void FillAttributeValue(
            const IE_NAMESPACE(partition)::PartitionIteratorPtr& partIter,
            const std::string& attrName, segmentid_t segmentId,
            const IE_NAMESPACE(partition)::AttributeDataPatcherPtr& attrPatcher);

public:
    static const std::string MERGER_NAME;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AlterDefaultFieldMerger);

}
}

#endif //ISEARCH_BS_ALTERDEFAULTFIELDMERGER_H
