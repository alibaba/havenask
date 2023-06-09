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

#include <map>
#include <memory>
#include <string>

#include "autil/Log.h"
#include "autil/mem_pool/ChunkAllocatorBase.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"
#include "indexlib/index/inverted_index/format/PostingFormat.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::file_system {
class RelocatableFolder;
}

namespace indexlibv2::framework {
class SegmentInfo;
struct SegmentMeta;
class IndexTaskResourceManager;
} // namespace indexlibv2::framework

namespace indexlibv2::config {
class IIndexConfig;
class TruncateOptionConfig;
} // namespace indexlibv2::config

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlibv2::index {
class DocMapper;
} // namespace indexlibv2::index

namespace indexlibv2::index {
class DocMapper;
} // namespace indexlibv2::index

namespace indexlib::index {
class BucketMap;
class TruncateIndexWriter;
class IndexOutputSegmentResource;
class IndexTermExtender;
class OnDiskIndexIteratorCreator;
class PostingMerger;
class MultiAdaptiveBitmapIndexWriter;
struct PostingWriterResource;

class InvertedIndexMerger : public indexlibv2::index::IIndexMerger
{
public:
    InvertedIndexMerger() = default;
    virtual ~InvertedIndexMerger();

    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;

    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager) override;
    virtual std::string GetIdentifier() const = 0;
    void SetPatchInfos(const indexlibv2::index::PatchInfos& patchInfos);

protected:
    virtual std::shared_ptr<OnDiskIndexIteratorCreator> CreateOnDiskIndexIteratorCreator() = 0;
    virtual PostingMerger*
    CreatePostingMerger(const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);
    virtual PostingMerger*
    CreateBitmapPostingMerger(const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);
    virtual void PrepareIndexOutputSegmentResource(
        const std::vector<SourceSegment>& srcSegments,
        const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);

    std::pair<Status, int64_t>
    GetMaxLengthOfPosting(const std::vector<indexlibv2::index::IIndexMerger::SourceSegment>& srcSegments) const;
    std::pair<Status, int64_t> GetMaxLengthOfPosting(std::shared_ptr<file_system::Directory> segDir,
                                                     const indexlibv2::framework::SegmentInfo& segInfo) const;
    int64_t
    GetHashDictMaxMemoryUse(const std::vector<indexlibv2::index::IIndexMerger::SourceSegment>& srcSegments) const;
    virtual std::shared_ptr<file_system::Directory>
    GetIndexDirectory(const std::shared_ptr<file_system::Directory>& segDir) const;
    virtual Status DoMerge(const SegmentMergeInfos& segMergeInfos,
                           const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager);

    IndexFormatOption _indexFormatOption;
    file_system::IOConfig _ioConfig;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::string _indexName;
    // TODO(yonghao.fyh): rethink naming
    util::SimplePool _simplePool;
    std::vector<std::shared_ptr<IndexOutputSegmentResource>> _indexOutputSegmentResources;

private:
    bool NeedPreloadMaxDictCount(size_t targetSegmentCount) const;
    size_t GetDictKeyCount(const std::vector<SourceSegment>& srcSegments) const;
    void EndMerge();
    Status MergePatches(const SegmentMergeInfos& segmentMergeInfos);
    Status MergeTerm(DictKeyInfo key, const SegmentTermInfos& segTermInfos, SegmentTermInfo::TermIndexMode mode,
                     const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
                     const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);

    std::shared_ptr<PostingMerger>
    MergeTermPosting(const SegmentTermInfos& segTermInfos, SegmentTermInfo::TermIndexMode mode,
                     const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
                     const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);

    Status InitAdaptiveBitmapIndexWriter(
        const std::shared_ptr<file_system::RelocatableFolder>& relocatableGlobalRoot,
        const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
        const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);

    Status LoadBucketMaps(const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager,
                          const std::shared_ptr<indexlibv2::config::TruncateOptionConfig>& truncateOptionConfig,
                          std::map<std::string, std::shared_ptr<BucketMap>>& bucketMaps);
    Status InitTruncateIndexWriter(
        const SegmentMergeInfos& segmentMergeInfos, const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
        const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager);
    Status InitWithoutParam(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);

private:
    std::unique_ptr<PostingFormat> _postingFormat;
    std::unique_ptr<autil::mem_pool::ChunkAllocatorBase> _allocator;
    std::unique_ptr<autil::mem_pool::Pool> _byteSlicePool;
    std::unique_ptr<autil::mem_pool::RecyclePool> _bufferPool;

    std::shared_ptr<PostingWriterResource> _postingWriterResource;
    std::string _docMapperName;
    std::shared_ptr<IndexTermExtender> _termExtender;
    std::shared_ptr<TruncateIndexWriter> _truncateIndexWriter;
    std::shared_ptr<MultiAdaptiveBitmapIndexWriter> _adaptiveBitmapIndexWriter;

    indexlibv2::index::PatchInfos _patchInfos;

    bool _isOptimizeMerge = false;

    std::map<std::string, std::any> _params;
    std::map<std::string, std::shared_ptr<BucketMap>> _bucketMaps;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
