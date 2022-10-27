#ifndef __INDEXLIB_ATTRIBUTE_MERGER_H
#define __INDEXLIB_ATTRIBUTE_MERGER_H

#include <queue>
#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_resource.h"
#include "indexlib/index/merger_resource.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/attribute_config.h"

DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);

IE_NAMESPACE_BEGIN(index);

#define DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(id)                         \
    static std::string Identifier() { return std::string("attribute.merger." #id);} \
    std::string GetIdentifier() const override{ return Identifier();}

class AttributeMerger
{
public:
    AttributeMerger(bool needMergePatch = true)
        : mNeedMergePatch(needMergePatch)
    {}
    
    virtual ~AttributeMerger(){}

public:
    void Init(const config::AttributeConfigPtr& attrConfig,
              const index_base::MergeItemHint& hint,
              const index_base::MergeTaskResourceVector& taskResources)
    {
        mMergeHint = hint;
        mTaskResources = taskResources;
        Init(attrConfig);
    }
    
    virtual void Init(const config::AttributeConfigPtr& attrConfig);

    virtual void BeginMerge(const SegmentDirectoryBasePtr& segDir);

    virtual void
    Merge(const MergerResource &resource,
          const index_base::SegmentMergeInfos &segMergeInfos,
          const index_base::OutputSegmentMergeInfos &outputSegMergeInfos) = 0;

    virtual void SortByWeightMerge(
        const MergerResource &resource,
        const index_base::SegmentMergeInfos &segMergeInfos,
        const index_base::OutputSegmentMergeInfos &outputSegMergeInfos) = 0;

    virtual int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge) const = 0;

    /////////

    virtual std::string GetIdentifier() const = 0;

    virtual bool EnableMultiOutputSegmentParallel() const { return true; }    

    virtual std::vector<index_base::ParallelMergeItem> CreateParallelMergeItems(
        const SegmentDirectoryBasePtr& segDir,
        const index_base::SegmentMergeInfos& inPlanSegMergeInfos, uint32_t instanceCount,
        bool isEntireDataSet, index_base::MergeTaskResourceManagerPtr& resMgr) const;

    virtual void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        int32_t totalParallelCount,
        const std::vector<index_base::MergeTaskResourceVector>& instResourceVec);

public:
    config::AttributeConfigPtr GetAttributeConfig() const;

protected:
    std::string GetAttributePath(const std::string &dir) const;

    template <typename MergerType, typename ConfigType = config::AttributeConfigPtr>
    void DoMergePatches(const MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, const ConfigType& config)
    {
        if (!mNeedMergePatch)
        {
            return;
        }
        file_system::DirectoryPtr targetDir;
        for (const auto& outputSeg : outputSegMergeInfos)
        {
            if (static_cast<size_t>(outputSeg.targetSegmentIndex) + 1 == resource.targetSegmentCount)
            {
                targetDir = outputSeg.directory;
                break;
            }
        }
        if (!targetDir)
        {
            return;
        }

        MergerType merger(config);
        merger.Merge(mSegmentDirectory->GetPartitionData(), segMergeInfos,
                     targetDir->GetDirectory(mAttributeConfig->GetAttrName(), true));
    }

protected:
    struct SortMergeItem
    {
        docid_t newDocId;
        segmentid_t segmentId;

        SortMergeItem(docid_t newId, segmentid_t segId)
            : newDocId(newId)
            , segmentId(segId)
        {
        }
    };

    struct SortMergeItemCompare
    {
        bool operator ()(const SortMergeItem& left, const SortMergeItem& right)
        {
            return left.newDocId > right.newDocId;
        }
    };

    typedef std::priority_queue<SortMergeItem, std::vector<SortMergeItem>,
                                SortMergeItemCompare> SortMergeItemHeap;

protected:
    SegmentDirectoryBasePtr mSegmentDirectory;
    config::AttributeConfigPtr mAttributeConfig;
    bool mNeedMergePatch;
    
    index_base::MergeItemHint mMergeHint;
    index_base::MergeTaskResourceVector mTaskResources;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_MERGER_H
