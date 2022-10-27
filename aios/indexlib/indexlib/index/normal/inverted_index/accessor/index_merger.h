#ifndef __INDEXLIB_INDEX_MERGER_H
#define __INDEXLIB_INDEX_MERGER_H

#include <tr1/memory>
#include <sstream>
#include <autil/ChunkAllocator.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/SynchronizedQueue.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_dumper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info.h"
#include "indexlib/index/merger_resource.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/util/simple_pool.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/normal/inverted_index/accessor/index_output_segment_resource.h"

DECLARE_REFERENCE_CLASS(index, IndexTermExtender);
DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index, OnDiskIndexIteratorCreator);
DECLARE_REFERENCE_CLASS(index, PostingMerger);
DECLARE_REFERENCE_CLASS(index, DictionaryWriter);
DECLARE_REFERENCE_CLASS(index, TruncateIndexWriter);
DECLARE_REFERENCE_CLASS(index, AdaptiveBitmapIndexWriter);
DECLARE_REFERENCE_CLASS(index, PostingFormat);
DECLARE_REFERENCE_CLASS(index, PostingWriterResource);
DECLARE_REFERENCE_CLASS(index, TruncateIndexWriterCreator);
DECLARE_REFERENCE_CLASS(index, AdaptiveBitmapIndexWriterCreator);
DECLARE_REFERENCE_CLASS(index, MultiAdaptiveBitmapIndexWriter);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, ParallelMergeItem);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

#define DECLARE_INDEX_MERGER_IDENTIFIER(id)                             \
    static std::string Identifier()                                     \
    {                                                                   \
        return std::string("index.merger.") + std::string(#id);         \
    }                                                                   \
    std::string GetIdentifier() const override { return Identifier();}

class IndexMerger
{
public:
    IndexMerger();
    virtual ~IndexMerger();

public:
    virtual void Init(const config::IndexConfigPtr& indexConfig,
                      const index_base::MergeItemHint& hint,
                      const index_base::MergeTaskResourceVector& taskResources,
                      const config::MergeIOConfig& ioConfig,
                      TruncateIndexWriterCreator* truncateCreator = nullptr,
                      AdaptiveBitmapIndexWriterCreator* adaptiveCreator = nullptr);

    virtual void BeginMerge(const SegmentDirectoryBasePtr& segDir);
    virtual void Merge(
            const index::MergerResource& resource,
            const index_base::SegmentMergeInfos& segMergeInfos,
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    virtual void SortByWeightMerge(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    virtual int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, bool isSortedMerge);

    virtual void SetReadBufferSize(uint32_t bufSize)
    {
        mIOConfig.readBufferSize = bufSize;
    }


    virtual std::string GetIdentifier() const = 0;

    virtual bool EnableMultiOutputSegmentParallel() const { return false; }

    virtual std::vector<index_base::ParallelMergeItem> CreateParallelMergeItems(
        const SegmentDirectoryBasePtr& segDir,
        const index_base::SegmentMergeInfos& inPlanSegMergeInfos,
        uint32_t instanceCount,bool isEntireDataSet,
        index_base::MergeTaskResourceManagerPtr& resMgr) const;

    virtual void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        int32_t totalParallelCount,
        const std::vector<index_base::MergeTaskResourceVector>& instResourceVec);

    IndexOutputSegmentResources& GetIndexOutputSegmentResources()
    {
        return mIndexOutputSegmentResources;
    }

    config::IndexConfigPtr GetIndexConfig() const { return mIndexConfig; }

protected:
    virtual void Init(const config::IndexConfigPtr& indexConfig);
    virtual void EndMerge();

    void InnerMerge(const index::MergerResource& resource,
                    const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);
    void MergeTerm(dictkey_t key, 
                   const SegmentTermInfos& segTermInfos,
                   SegmentTermInfo::TermIndexMode mode, 
                   const index::MergerResource& resource,
                   const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    void DoMergeTerm(dictkey_t key, const SegmentTermInfos& segTermInfos,
                     SegmentTermInfo::TermIndexMode mode, const index::MergerResource& resource,
                     const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);

    PostingMergerPtr MergeTermPosting(const SegmentTermInfos& segTermInfos,
            SegmentTermInfo::TermIndexMode mode, 
            const index::MergerResource& resource,
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    void ResetMemPool();
    int64_t GetMaxLengthOfPosting(const SegmentDirectoryBasePtr& segDir,
                                  const index_base::SegmentMergeInfos& segMergeInfos) const;

    int64_t GetHashDictMaxMemoryUse(const SegmentDirectoryBasePtr& segDir,
                                    const index_base::SegmentMergeInfos& segMergeInfos) const;

    // virtual void SetMergeIOConfig(const config::MergeIOConfig &ioConfig)
    // {
    //     mIOConfig = ioConfig;
    // }

protected:
    virtual OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() = 0;
    virtual PostingMerger* CreatePostingMerger(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    virtual PostingMerger* CreateBitmapPostingMerger(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    virtual int64_t GetMaxLengthOfPosting(const index_base::SegmentData& segData) const;
    // std::string MakeMergedDir(const std::string& directory,
    //                     config::IndexConfigPtr& indexConfig, 
    //                     index::IndexFormatOption& indexFormatOption,
    //                     index_base::MergeItemHint& mergeHint);
    file_system::DirectoryPtr MakeMergeDir(const file_system::DirectoryPtr& indexDirectory,
            config::IndexConfigPtr& indexConfig,
            index::IndexFormatOption& indexFormatOption,
            index_base::MergeItemHint& mergeHint);
    
    virtual void PrepareIndexOutputSegmentResource(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    bool NeedPreloadMaxDictCount(
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) const;
    
protected:
    // ReclaimMapPtr GetReclaimMap() const
    // {
    //     return mResource.reclaimMap;
    // }

    std::string GetMergedDir(const std::string& rootDir) const;

    file_system::DirectoryPtr GetMergeDir(const file_system::DirectoryPtr& indexDirectory,
            bool needClear) const;

    const SegmentDirectoryBasePtr& GetSegmentDirectory() const { return mSegmentDirectory; }

    autil::mem_pool::ChunkAllocatorBase* GetAllocator() const { return mAllocator; }

    autil::mem_pool::Pool* GetByteSlicePool() const { return mByteSlicePool; }
    autil::mem_pool::RecyclePool* GetBufferPool() const { return mBufferPool; }

    const index::PostingFormatOption& GetPostingFormatOption() const
    {
        return mIndexFormatOption.GetPostingFormatOption();
    }

    autil::mem_pool::Pool* AllocateByteSlicePool();
    autil::mem_pool::RecyclePool* AllocateBufferPool();

    size_t GetDictKeyCount(const index_base::SegmentMergeInfos& segMergeInfos) const;

protected:
    SegmentDirectoryBasePtr mSegmentDirectory;
    config::IndexConfigPtr mIndexConfig;
    index::IndexFormatOption mIndexFormatOption;
    index::PostingFormat* mPostingFormat;

    config::MergeIOConfig mIOConfig;

    autil::mem_pool::ChunkAllocatorBase* mAllocator;
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    util::SimplePool mSimplePool;
    PostingWriterResourcePtr mPostingWriterResource;
    bool mSortMerge;

    index_base::MergeItemHint mMergeHint;
    index_base::MergeTaskResourceVector mTaskResources;
    IndexOutputSegmentResources mIndexOutputSegmentResources;
    IndexTermExtenderPtr mIndexTermExtender;
    TruncateIndexWriterPtr mTruncateIndexWriter;
    MultiAdaptiveBitmapIndexWriterPtr mAdaptiveBitmapIndexWriter;
    IndexTermExtenderPtr mTermExtender;

private:
    friend class IndexMergerTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_MERGER_H
