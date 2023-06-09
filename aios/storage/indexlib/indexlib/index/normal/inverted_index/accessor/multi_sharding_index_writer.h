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

#include "indexlib/index/inverted_index/format/ShardingIndexHasher.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"

DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
namespace indexlib { namespace index {

class MultiShardingIndexWriter : public index::IndexWriter
{
public:
    MultiShardingIndexWriter(segmentid_t segmentId, const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics,
                             const config::IndexPartitionOptions& options);

    ~MultiShardingIndexWriter() {}

public:
    void Init(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics) override;

    size_t EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter = index_base::PartitionSegmentIteratorPtr()) override;

    void AddField(const document::Field* field) override;
    void UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    void UpdateField(docid_t docId, index::DictKeyInfo termKey, bool isDelete) override;

    void EndDocument(const document::IndexDocument& indexDocument) override;

    void EndSegment() override;

    void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) override;

    uint64_t GetNormalTermDfSum() const override;
    void FillDistinctTermCount(std::shared_ptr<framework::SegmentMetrics> _segmentMetrics) override;
    index::IndexSegmentReaderPtr CreateInMemReader() override;

    void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                               std::priority_queue<size_t>& minMemUses) const override;

    IndexWriter* GetIndexWriter(uint32_t shardId) const;

    void AddField(const document::Field* field, size_t shardId);
    void UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens, size_t shardId);
    void UpdateField(docid_t docId, index::DictKeyInfo termKey, bool isDelete, size_t shardId);
    void EndDocument(const document::IndexDocument& indexDocument, size_t shardId);

public: // for test
    std::vector<NormalIndexWriterPtr>& GetShardingIndexWriters() { return _shardingWriters; }

    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<std::unique_ptr<common::DumpItem>>& dumpItems);

private:
    void UpdateBuildResourceMetrics() override {};

    void AddToken(const document::Token* token, fieldid_t fieldId, pos_t tokenBasePos);
    void AddToken(const document::Token* token, fieldid_t fieldId, pos_t tokenBasePos, size_t shardId);
    uint32_t GetDistinctTermCount() const override;

private:
    segmentid_t _segmentId;
    pos_t _basePos;
    std::vector<NormalIndexWriterPtr> _shardingWriters;
    std::unique_ptr<index::SectionAttributeWriter> _sectionAttributeWriter;
    std::unique_ptr<ShardingIndexHasher> _shardingIndexHasher;
    std::shared_ptr<framework::SegmentMetrics> _segmentMetrics;
    config::IndexPartitionOptions _options;

private:
    friend class IndexReaderTestBase;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiShardingIndexWriter);
}} // namespace indexlib::index
