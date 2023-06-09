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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::index {
class DocMapper;
class PatchFileInfo;
} // namespace indexlibv2::index

namespace indexlib::index {
class IndexOutputSegmentResource;

class SimpleInvertedIndexMerger : private autil::NoCopyable
{
public:
    SimpleInvertedIndexMerger(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);
    ~SimpleInvertedIndexMerger() {};

public:
    Status Merge(const std::shared_ptr<file_system::Directory>& inputIndexDir,
                 const std::shared_ptr<indexlibv2::index::PatchFileInfo>& inputPatchFileInfo,
                 const std::shared_ptr<file_system::Directory>& outputIndexDir, size_t dictKeyCount, uint64_t docCount);
    int64_t CurrentMemUse();

private:
    Status DoMerge(const std::shared_ptr<file_system::Directory>& inputIndexDir,
                   const std::shared_ptr<indexlibv2::index::PatchFileInfo>& inputPatchFileInfo,
                   const std::shared_ptr<file_system::Directory>& outputIndexDir, size_t dictKeyCount,
                   uint64_t docCount);
    void PrepareIndexOutputSegmentResource(const std::shared_ptr<file_system::Directory>& outputDir,
                                           size_t dictKeyCount);
    void MergeTerm(DictKeyInfo key, const SegmentTermInfos& segTermInfos,
                   const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper, SegmentTermInfo::TermIndexMode mode,
                   const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);

public:
    util::SimplePool _simplePool;
    std::unique_ptr<util::MMapAllocator> _allocator;
    std::unique_ptr<autil::mem_pool::Pool> _byteSlicePool;
    std::unique_ptr<autil::mem_pool::RecyclePool> _bufferPool;

    std::unique_ptr<PostingWriterResource> _postingWriterResource;
    std::vector<std::shared_ptr<IndexOutputSegmentResource>> _indexOutputSegmentResources;

    IndexFormatOption _indexFormatOption;
    file_system::IOConfig _ioConfig;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
