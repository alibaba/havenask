#ifndef __INDEXLIB_INDEX_REDUCER_H
#define __INDEXLIB_INDEX_REDUCER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"

DECLARE_REFERENCE_CLASS(index, IndexReduceItem);

IE_NAMESPACE_BEGIN(index);

class ParallelReduceMeta : public autil::legacy::Jsonizable
{
public:
    ParallelReduceMeta(uint32_t _parallelCount)
        : parallelCount(_parallelCount)
    {}
    ParallelReduceMeta()
        : parallelCount(0)
    {}
    
public:
    static std::string GetFileName() { return std::string("parallel_meta"); }
    std::string GetInsDirName(uint32_t id);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("parallel_count", parallelCount, parallelCount);
    }
    
    void Store(const std::string& dir) const;
    bool Load(const std::string& path);
    void Store(const file_system::DirectoryPtr& directory) const;
    bool Load(const file_system::DirectoryPtr& directory);

public:
    uint32_t parallelCount;
};

class ReduceTask
{
public:
    // reduce task id
    int32_t id;
    // 'dataRatio' describes how much percent of data will be processed by this sub reduce task.
    // this parameter is used to distribute reduce task.
    float dataRatio;
    // 'resourceIds' describes how many resources is needed by this reduce task.
    // User plugin should use MergeTaskResourceManager to declare resources.
    std::vector<index_base::resourceid_t> resourceIds;
};

class ReduceResource
{
public:
    ReduceResource(const index_base::SegmentMergeInfos& _segMergeInfos,
                   const ReclaimMapPtr& _reclaimMap,
                   bool _isEntireDataSet);
    ~ReduceResource();
    const index_base::SegmentMergeInfos& segMergeInfos;
    const ReclaimMapPtr& reclaimMap;
    bool isEntireDataSet;
};

class IndexReducer
{
public:
    IndexReducer(const util::KeyValueMap& parameters)
        : mParameters(parameters)
    {}
    virtual ~IndexReducer() {}
public:
    bool Init(const IndexerResourcePtr& resource,
              const index_base::MergeItemHint& mergeHint,
              const index_base::MergeTaskResourceVector& taskResources);
              
    virtual bool DoInit(const IndexerResourcePtr& resource) = 0; 
    
    virtual IndexReduceItemPtr CreateReduceItem() = 0;

    virtual bool Reduce(
            const std::vector<IndexReduceItemPtr>& reduceItems,
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
            bool isSortMerge,
            const ReduceResource& resource) = 0;

    // avoid actual reading index files in this interface
    virtual int64_t EstimateMemoryUse(
            const std::vector<file_system::DirectoryPtr>& indexDirs,
            const index_base::SegmentMergeInfos& segmentInfos,
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments            
            bool isSortMerge) const = 0;

    virtual bool EnableMultiOutputSegmentParallel() const { return false; }

    // by default, return empty ReduceTask
    virtual std::vector<ReduceTask> CreateReduceTasks(
        const std::vector<file_system::DirectoryPtr>& indexDirs,
        const index_base::SegmentMergeInfos& segmentInfos, uint32_t instanceCount,
        bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr);

    // should be re-accessable in EndMerge phrase
    virtual void EndParallelReduce(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        int32_t totalParallelCount,
        const std::vector<index_base::MergeTaskResourceVector>& instResourceVec);

protected:
    util::KeyValueMap mParameters;
    index_base::MergeItemHint mMergeHint;
    index_base::MergeTaskResourceVector mTaskResources;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexReducer);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_REDUCER_H
