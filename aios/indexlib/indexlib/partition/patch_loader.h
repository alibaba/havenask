#ifndef __INDEXLIB_PATCH_LOADER_H
#define __INDEXLIB_PATCH_LOADER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_work_item.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(index, PatchIterator);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(util, ThreadPool);
DECLARE_REFERENCE_CLASS(tools, PatchTools);

IE_NAMESPACE_BEGIN(partition);

class PatchLoader
{
public:
    PatchLoader(const config::IndexPartitionSchemaPtr& schema,
                const config::OnlineConfig& onlineConfig);
    ~PatchLoader();

public:

    void Init(const index_base::PartitionDataPtr& partitionData,
              bool isIncConsistentWithRt,
              const index_base::Version& lastLoadedVersion,
              segmentid_t startLoadSegment,
              bool loadPatchForOpen);

    void Load(const index_base::Version& patchLoadedVersion,
              const PartitionModifierPtr& modifier);

    size_t CalculatePatchLoadExpandSize();

    // only for test
    static void LoadAttributePatch(
            index::AttributeReader& attrReader,
            const config::AttributeConfigPtr& attrConfig,
            const index_base::PartitionDataPtr& partitionData);

private:
    bool NeedLoadPatch(const index_base::Version& currentVersion,
                       const index_base::Version& loadedVersion) const;
    static bool HasUpdatableAttribute(
            const config::IndexPartitionSchemaPtr& schema);

    size_t SingleThreadLoadPatch(
        const PartitionModifierPtr& modifier);

    size_t MultiThreadLoadPatch(
        const PartitionModifierPtr& modifier);
    void CreatePatchWorkItems(const index::PatchIteratorPtr& iter,
                              std::vector<index::AttributePatchWorkItem*>& patchWorkItems);
    bool InitPatchWorkItems(const PartitionModifierPtr& modifier,
                            std::vector<index::AttributePatchWorkItem*>& patchWorkItems);
    size_t EstimatePatchWorkItemCost(std::vector<index::AttributePatchWorkItem*>& patchWorkItems,
                                     const util::ThreadPoolPtr& threadPool);
    void SortPatchWorkItemsByCost(std::vector<index::AttributePatchWorkItem*>& patchWorkItems);

private:
    static int64_t PATCH_WORK_ITEM_COST_SAMPLE_COUNT;
    
private:
    index_base::PartitionDataPtr mPartitionData;
    config::IndexPartitionSchemaPtr mSchema;
    config::OnlineConfig mOnlineConfig;
    index::PatchIteratorPtr mIter;
    std::vector<index::AttributePatchWorkItem*> mPatchWorkItems;
private:
    friend class IE_NAMESPACE(tools)::PatchTools;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchLoader);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PATCH_LOADER_H
