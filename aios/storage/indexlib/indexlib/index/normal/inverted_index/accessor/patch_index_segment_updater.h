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
#include <unordered_map>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/format/ComplexDocid.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

DECLARE_REFERENCE_CLASS(document, ModifiedTokens);

namespace indexlib { namespace index {

class PatchIndexSegmentUpdaterBase
{
public:
    virtual ~PatchIndexSegmentUpdaterBase() {}
    virtual void Update(docid_t docId, const document::ModifiedTokens& modifiedTokens) = 0;
    virtual void Dump(const file_system::DirectoryPtr& indexDir, segmentid_t srcSegment) = 0;

public:
    static std::string GetPatchFileName(segmentid_t srcSegment, segmentid_t destSegment);
};

class PatchIndexSegmentUpdater : public PatchIndexSegmentUpdaterBase
{
private:
    using VectorAllocator = autil::mem_pool::pool_allocator<ComplexDocId>;
    using DocVector = std::vector<ComplexDocId, VectorAllocator>;

    using MapAllocator = autil::mem_pool::pool_allocator<std::pair<const uint64_t, DocVector>>;
    using HashMap =
        std::unordered_map<const uint64_t, DocVector, std::hash<uint64_t>, std::equal_to<uint64_t>, MapAllocator>;

public:
    PatchIndexSegmentUpdater(util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
                             const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);

public:
    void Update(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    void Dump(const file_system::DirectoryPtr& indexDir, segmentid_t srcSegment) override;
    void DumpToFieldDirectory(const file_system::DirectoryPtr& indexFieldDir, segmentid_t srcSegment);

    void Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete);
    void UpdateBuildResourceMetrics();

private:
    static file_system::FileWriterPtr CreateFileWriter(const file_system::DirectoryPtr& directory,
                                                       const std::string& fileName, bool hasPatchCompress);

private:
    util::SimplePool _simplePool;
    util::BuildResourceMetrics* _buildResourceMetrics;
    util::BuildResourceMetricsNode* _buildResourceMetricsNode;
    segmentid_t _segmentId;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    HashMap _hashMap;
    DocVector _nullTermDocs;
    size_t _docCount;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(PatchIndexSegmentUpdaterBase);
DEFINE_SHARED_PTR(PatchIndexSegmentUpdater);
}} // namespace indexlib::index
