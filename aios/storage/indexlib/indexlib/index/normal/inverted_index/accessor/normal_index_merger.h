/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <sstream>

#include "autil/ChunkAllocator.h"
#include "autil/SynchronizedQueue.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/normal/framework/index_merger.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index/util/merger_resource.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"

DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index, PostingMerger);
DECLARE_REFERENCE_CLASS(index, DictionaryWriter);
DECLARE_REFERENCE_CLASS(index::legacy, TruncateIndexWriter);
DECLARE_REFERENCE_CLASS(index::legacy, TruncateIndexWriterCreator);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, MergeTaskResourceManager);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(tools, IndexPrinterWorker);

namespace indexlib { namespace index { namespace legacy {
class AdaptiveBitmapIndexWriter;
class OnDiskIndexIteratorCreator;
class AdaptiveBitmapIndexWriterCreator;
class MultiAdaptiveBitmapIndexWriter;
class IndexTermExtender;
}}} // namespace indexlib::index::legacy

namespace indexlib { namespace index {
struct PostingWriterResource;
class PostingFormat;

#define DECLARE_INDEX_MERGER_IDENTIFIER(id)                                                                            \
    static std::string Identifier() { return std::string("index.merger.") + std::string(#id); }                        \
    std::string GetIdentifier() const override { return Identifier(); }

class NormalIndexMerger : public IndexMerger
{
public:
    NormalIndexMerger();
    virtual ~NormalIndexMerger();

public:
    void Init(const config::IndexConfigPtr& indexConfig, const index_base::MergeItemHint& hint,
              const index_base::MergeTaskResourceVector& taskResources, const config::MergeIOConfig& ioConfig,
              legacy::TruncateIndexWriterCreator* truncateCreator = nullptr,
              legacy::AdaptiveBitmapIndexWriterCreator* adaptiveCreator = nullptr) override;

    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    void SortByWeightMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override;

    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) const override;

    virtual void SetReadBufferSize(uint32_t bufSize) { mIOConfig.readBufferSize = bufSize; }

    virtual std::string GetIdentifier() const override = 0;

    IndexOutputSegmentResources& GetIndexOutputSegmentResources() { return mIndexOutputSegmentResources; }

    config::IndexConfigPtr GetIndexConfig() const { return mIndexConfig; }

protected:
    virtual void Init(const config::IndexConfigPtr& indexConfig);
    virtual void EndMerge();

    void InnerMerge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);
    void MergeTerm(index::DictKeyInfo key, const SegmentTermInfos& segTermInfos, SegmentTermInfo::TermIndexMode mode,
                   const index::MergerResource& resource,
                   const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    void DoMergeTerm(index::DictKeyInfo key, const SegmentTermInfos& segTermInfos, SegmentTermInfo::TermIndexMode mode,
                     const index::MergerResource& resource,
                     const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);

    PostingMergerPtr MergeTermPosting(const SegmentTermInfos& segTermInfos, SegmentTermInfo::TermIndexMode mode,
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
    virtual std::shared_ptr<legacy::OnDiskIndexIteratorCreator> CreateOnDiskIndexIteratorCreator() = 0;
    virtual PostingMerger* CreatePostingMerger(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    virtual PostingMerger*
    CreateBitmapPostingMerger(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    virtual int64_t GetMaxLengthOfPosting(const index_base::SegmentData& segData) const;
    // std::string MakeMergedDir(const std::string& directory,
    //                     config::IndexConfigPtr& indexConfig,
    //                     index::IndexFormatOption& indexFormatOption,
    //                     index_base::MergeItemHint& mergeHint);
    file_system::DirectoryPtr MakeMergeDir(const file_system::DirectoryPtr& indexDirectory,
                                           config::IndexConfigPtr& indexConfig,
                                           LegacyIndexFormatOption& indexFormatOption,
                                           index_base::MergeItemHint& mergeHint);

    virtual void PrepareIndexOutputSegmentResource(const index::MergerResource& resource,
                                                   const index_base::SegmentMergeInfos& segMergeInfos,
                                                   const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

    bool NeedPreloadMaxDictCount(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) const;

protected:
    // ReclaimMapPtr GetReclaimMap() const
    // {
    //     return mResource.reclaimMap;
    // }

    autil::mem_pool::ChunkAllocatorBase* GetAllocator() const { return mAllocator; }

    autil::mem_pool::Pool* GetByteSlicePool() const { return mByteSlicePool; }
    autil::mem_pool::RecyclePool* GetBufferPool() const { return mBufferPool; }

    const PostingFormatOption& GetPostingFormatOption() const { return mIndexFormatOption.GetPostingFormatOption(); }

    autil::mem_pool::Pool* AllocateByteSlicePool();
    autil::mem_pool::RecyclePool* AllocateBufferPool();
    void MergePatches(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                      const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                      const config::IndexConfigPtr& config);

    size_t GetDictKeyCount(const index_base::SegmentMergeInfos& segMergeInfos) const;

protected:
    LegacyIndexFormatOption mIndexFormatOption;
    PostingFormat* mPostingFormat;

    autil::mem_pool::ChunkAllocatorBase* mAllocator;
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    util::SimplePool mSimplePool;
    std::shared_ptr<PostingWriterResource> mPostingWriterResource;
    bool mSortMerge;

    IndexOutputSegmentResources mIndexOutputSegmentResources;
    std::shared_ptr<legacy::IndexTermExtender> mIndexTermExtender;
    std::shared_ptr<legacy::TruncateIndexWriter> mTruncateIndexWriter;
    std::shared_ptr<legacy::MultiAdaptiveBitmapIndexWriter> mAdaptiveBitmapIndexWriter;
    std::shared_ptr<legacy::IndexTermExtender> mTermExtender;

private:
    friend class NormalIndexMergerTest;
    friend class tools::IndexPrinterWorker;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalIndexMerger);
}} // namespace indexlib::index
