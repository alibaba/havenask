#ifndef ISEARCH_BS_NEWFIELDMERGETASK_H
#define ISEARCH_BS_NEWFIELDMERGETASK_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/task_base/TaskBase.h"
#include "build_service/config/OfflineMergeConfig.h"
#include "build_service/custom_merger/CustomMergerCreator.h"
#include "build_service/custom_merger/CustomMergeMeta.h"
#include <indexlib/index_base/index_meta/parallel_build_info.h>

namespace build_service {
namespace task_base {

class NewFieldMergeTask : public TaskBase
{
public:
    NewFieldMergeTask(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);
    ~NewFieldMergeTask();
private:
    NewFieldMergeTask(const NewFieldMergeTask &);
    NewFieldMergeTask& operator=(const NewFieldMergeTask &);
public:
    bool init(const std::string &jobParam) override;
    bool run(uint32_t instanceId, Mode mode);
    int32_t getTargetVersionId() const { return _targetVersionId; }
protected:
    virtual custom_merger::MergeResourceProviderPtr prepareResourceProvider();
    bool prepareNewFieldMergeMeta(
            const custom_merger::MergeResourceProviderPtr& resourceProvider);
    bool beginMerge();
    bool doMerge(uint32_t instanceId);
    bool endMerge();
private:
    std::string getMergeMetaPath();
    custom_merger::CustomMergerPtr createCustomMerger();
    void mergeIncParallelBuildData(const IE_NAMESPACE(index_base)::ParallelBuildInfo& incBuildInfo);
    bool parseTargetDescription();
    
private:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _newSchema;
    config::OfflineMergeConfig _offlineMergeConfig;
    custom_merger::CustomMergerCreatorPtr _customMergerCreator;
    custom_merger::CustomMergerPtr _newFieldMerger;
    int32_t _targetVersionId;
    int32_t _alterFieldTargetVersionId;
    int32_t _checkpointVersionId;
    std::string _indexRoot;
    uint32_t _opsId;


private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(NewFieldMergeTask);

}
}

#endif //ISEARCH_BS_NEWFIELDMERGETASK_H
