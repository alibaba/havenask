#ifndef __INDEXLIB_PACK_INDEX_MERGER_H
#define __INDEXLIB_PACK_INDEX_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"

IE_NAMESPACE_BEGIN(index);

class PackIndexMerger : public IndexMerger
{
public:
    typedef AttributeMergerPtr SectionAttributeMergerPtr;

public:
    PackIndexMerger();
    ~PackIndexMerger();

    DECLARE_INDEX_MERGER_IDENTIFIER(pack);
    DECLARE_INDEX_MERGER_CREATOR(PackIndexMerger, it_pack);

public:
    void Init(const config::IndexConfigPtr& indexConfig) override;

    void BeginMerge(const SegmentDirectoryBasePtr& segDir) override;

    void Merge(
            const index::MergerResource& resource,
            const index_base::SegmentMergeInfos& segMergeInfos,
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    void SortByWeightMerge(const index::MergerResource& resource,
                           const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);



    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
                              const MergerResource& resource, 
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) override;

private:
    SectionAttributeMergerPtr mSectionMerger;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackIndexMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_INDEX_MERGER_H

