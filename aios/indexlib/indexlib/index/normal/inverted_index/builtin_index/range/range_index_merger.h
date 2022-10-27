#ifndef __INDEXLIB_RANGE_INDEX_MERGER_H
#define __INDEXLIB_RANGE_INDEX_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_info.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/on_disk_range_index_iterator_creator.h"

IE_NAMESPACE_BEGIN(index);

class RangeLevelIndexMerger : public IndexMerger
{
public:
    explicit RangeLevelIndexMerger()
    {}
    ~RangeLevelIndexMerger(){}

    DECLARE_INDEX_MERGER_IDENTIFIER(range_level);

public:
    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override
    {
        return OnDiskIndexIteratorCreatorPtr(
                new OnDiskRangeIndexIteratorCreator(
                        GetPostingFormatOption(), mIOConfig,
                        mIndexConfig, mParentIndexName));
    }

    void Init(const std::string& parentIndexName,
              const config::IndexConfigPtr& indexConfig,
              const index_base::MergeItemHint& hint,
              const index_base::MergeTaskResourceVector& taskResources,
              const config::MergeIOConfig& ioConfig,
              TruncateIndexWriterCreator* truncateCreator,
              AdaptiveBitmapIndexWriterCreator* adaptiveCreator)
    {
        IndexMerger::Init(indexConfig, hint, taskResources, ioConfig, truncateCreator, adaptiveCreator);
        mParentIndexName = parentIndexName;
    }

    void PrepareIndexOutputSegmentResource(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;
    
private:
    void Init(const config::IndexConfigPtr& indexConfig) override
    {
        IndexMerger::Init(indexConfig);
    }

private:
    std::string mParentIndexName;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeLevelIndexMerger);

class RangeIndexMerger : public IndexMerger
{
public:
    DECLARE_INDEX_MERGER_IDENTIFIER(range);
    DECLARE_INDEX_MERGER_CREATOR(RangeIndexMerger, it_range);
public:
    RangeIndexMerger()
    {
        mBottomLevelIndexMerger.reset(new RangeLevelIndexMerger());
        mHighLevelIndexMerger.reset(new RangeLevelIndexMerger());
    }
    ~RangeIndexMerger() {}

public:
    void Init(const config::IndexConfigPtr& indexConfig,
              const index_base::MergeItemHint& hint,
              const index_base::MergeTaskResourceVector& taskResources,
              const config::MergeIOConfig& ioConfig,
              TruncateIndexWriterCreator* truncateCreator,
              AdaptiveBitmapIndexWriterCreator* adaptiveCreator);
    void BeginMerge(const SegmentDirectoryBasePtr& segDir) override;
    void Merge(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;
    
    void SortByWeightMerge(const index::MergerResource& resource,
                           const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;
    void SetReadBufferSize(uint32_t bufSize) override;

protected:
    void EndMerge() override
    {
        assert(false);
    }

    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override
    {
        assert(false);
        return OnDiskIndexIteratorCreatorPtr();
    }
    int64_t GetMaxLengthOfPosting(
            const index_base::SegmentData& segData) const override;

    void MergeRangeInfo(const index_base::SegmentMergeInfos& segMergeInfos,
                        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    void PrepareIndexOutputSegmentResource(
            const index::MergerResource& resource,
            const index_base::SegmentMergeInfos& segMergeInfos,
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;
    
private:
    RangeLevelIndexMergerPtr mBottomLevelIndexMerger;
    RangeLevelIndexMergerPtr mHighLevelIndexMerger;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeIndexMerger);
/////////////////////////////////////////////

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RANGE_INDEX_MERGER_H
