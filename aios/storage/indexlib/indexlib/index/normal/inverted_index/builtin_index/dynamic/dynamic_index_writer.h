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

#include "autil/mem_pool/RecyclePool.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/normal/inverted_index/accessor/patch_index_segment_updater.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/dynamic_index_posting_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/dynamic/in_mem_dynamic_index_segment_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/HashMap.h"

DECLARE_REFERENCE_CLASS(index, IndexSegmentReader);

namespace indexlib { namespace index {

class DynamicIndexWriter
{
    using PostingTable = util::HashMap<dictkey_t, PostingWriter*, autil::mem_pool::RecyclePool>;

public:
    struct DynamicPostingTable {
        autil::mem_pool::RecyclePool pool;
        PostingTable table;
        DynamicIndexPostingWriter* nullTermPosting;
        DynamicIndexPostingWriter::Stat stat;
        NodeManager nodeManager;

        DynamicPostingTable(size_t hashMapInitSize);
        ~DynamicPostingTable();
    };

public:
    DynamicIndexWriter(segmentid_t segmentId, size_t hashMapInitSize, bool isNumberIndex,
                       const config::IndexPartitionOptions& options);
    ~DynamicIndexWriter();

public:
    void Init(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics);
    void UpdateTerm(docid_t docId, index::DictKeyInfo termKey, document::ModifiedTokens::Operation op);
    InMemDynamicIndexSegmentReaderPtr CreateInMemReader();
    void Dump(const file_system::DirectoryPtr& fieldDir);
    void UpdateBuildResourceMetrics();

public:
    static void UpdateToken(index::DictKeyInfo termKey, bool isNumberIndex, DynamicPostingTable* postingTable,
                            docid_t docId, document::ModifiedTokens::Operation op);
    static DynamicIndexPostingWriter* CreatePostingWriter(DynamicPostingTable* postingTable);

public:
    DynamicIndexPostingWriter* TEST_GetPostingWriter(uint64_t key);
    void TEST_AddToken(const index::DictKeyInfo& hashKey, pospayload_t posPayload);

private:
    DynamicIndexPostingWriter* CreatePostingWriter();

private:
    segmentid_t _segmentId;
    size_t _hashMapInitSize;
    bool _isNumberIndex;
    config::IndexPartitionOptions _options;
    std::unique_ptr<DynamicPostingTable> _postingTable;
    util::BuildResourceMetricsNode* _buildResourceMetricsNode;
    std::unique_ptr<PatchIndexSegmentUpdater> _updater; // for offline
    std::shared_ptr<file_system::ResourceFile> _resource;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
