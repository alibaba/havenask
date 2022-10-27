#ifndef ISEARCH_BS_MERGETASK_H
#define ISEARCH_BS_MERGETASK_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/task_base/TaskBase.h"
#include <indexlib/merger/index_partition_merger.h>
#include <indexlib/config/index_partition_options.h>

DECLARE_REFERENCE_CLASS(util, CounterMap);
BS_DECLARE_REFERENCE_CLASS(config, CounterConfig);

namespace build_service {
namespace task_base {

class MergeTask : public TaskBase
{
private:
    enum MergeStep {
        MS_ALL,
        MS_INIT_MERGE,
        MS_DO_MERGE,
        MS_END_MERGE,
        MS_DO_NOTHING
    };
    static const uint32_t RESERVED_VERSION_NUM;

public:
    struct MergeStatus {
        versionid_t targetVersionId;
        bool isMergeFinished;
        MergeStatus()
            : targetVersionId(INVALID_VERSION)
            , isMergeFinished(false)
        {
        }
    };
    
public:
    MergeTask();
    MergeTask(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider);
    ~MergeTask();
private:
    MergeTask(const MergeTask &);
    MergeTask& operator=(const MergeTask &);
public:
    // virtual for mock
    virtual bool run(const std::string &jobParam, uint32_t instanceId,
                     Mode mode, bool optimize);
    
    virtual MergeStatus getMergeStatus() { return _mergeStatus; }
    
private:
    bool mergeSinglePart(const proto::PartitionId &partitionId,
                         const std::string &targetIndexPath,
                         const IE_NAMESPACE(config)::IndexPartitionOptions &options,
                         const std::pair<MergeStep, uint32_t> mergeStep,
                         bool optimize,
                         const config::CounterConfigPtr &counterConfig); 
    
    bool mergeMultiPart(const proto::PartitionId &partitionId,
                        const std::string &indexRoot,
                        const std::string &targetIndexPath,
                        const IE_NAMESPACE(config)::IndexPartitionOptions &options,
                        const std::pair<MergeStep, uint32_t> mergeStep,
                        bool optimize,
                        const config::CounterConfigPtr &counterConfig); 

    IE_NAMESPACE(merger)::IndexPartitionMergerPtr createSinglePartMerger(
        const proto::PartitionId &partitionId,        
        const std::string &targetIndexPath,
        const IE_NAMESPACE(config)::IndexPartitionOptions &options);
    
    bool doMerge(IE_NAMESPACE(merger)::IndexPartitionMergerPtr &indexPartMerger,
                 const std::pair<MergeStep, uint32_t> &mergeStep,
                 bool optimize, const proto::PartitionId &partitionId,
                 const std::string& targetIndexPath);

    static void removeObsoleteMergeMeta(const std::string &mergeMetaRoot,
            uint32_t reservedVersionNum = RESERVED_VERSION_NUM);
    virtual void cleanUselessResource();

    virtual IE_NAMESPACE(merger)::IndexPartitionMergerPtr CreateIncParallelPartitionMerger(
        const proto::PartitionId &partitionId,
        const std::string& targetIndexPath,
        const IE_NAMESPACE(config)::IndexPartitionOptions& options,
        uint32_t workerPathVersion);

private:
    bool isMultiPartMerge() const;
    std::pair<MergeStep, uint32_t> getMergeStep(Mode mode, uint32_t instanceId) const;
    std::string timeDiffToString(int64_t timeDiff);

protected:
    MergeStatus _mergeStatus;
    std::vector<std::string> _uselessPaths;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MergeTask);

}
}

#endif //ISEARCH_BS_MERGETASK_H
