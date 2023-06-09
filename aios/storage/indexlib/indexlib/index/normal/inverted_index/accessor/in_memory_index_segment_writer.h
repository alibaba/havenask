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

#include <list>
#include <memory>
#include <queue>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyWriter);
DECLARE_REFERENCE_CLASS(index, MultiFieldIndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, IndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, InMemMultiShardingIndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, BuildProfilingMetrics);
DECLARE_REFERENCE_CLASS(index, SectionAttributeWriter);

namespace indexlib { namespace index {

class InMemoryIndexSegmentWriter
{
public:
    InMemoryIndexSegmentWriter() {}
    ~InMemoryIndexSegmentWriter() {}

public:
    void Init(segmentid_t segmentId, const config::IndexPartitionSchemaPtr& schema,
              const config::IndexPartitionOptions& options,
              const std::shared_ptr<framework::SegmentMetrics>& lastSegmentMetrics,
              util::BuildResourceMetrics* buildResourceMetrics, const plugin::PluginManagerPtr& pluginManager,
              const index_base::PartitionSegmentIteratorPtr& segIter);

    void CollectSegmentMetrics();
    bool AddDocument(const document::NormalDocumentPtr& doc);
    // must be thread-safe
    void AddDocumentByIndex(const document::NormalDocumentPtr& doc, indexid_t indexId, size_t shardId);
    bool UpdateDocument(docid_t docId, const document::NormalDocumentPtr& doc);
    // must be thread-safe
    bool UpdateDocumentByIndex(docid_t docId, const document::NormalDocumentPtr& doc, indexid_t indexId,
                               size_t shardId);
    void EndDocument(const document::IndexDocumentPtr& indexDocument);
    void EndDocumentByIndex(const document::IndexDocumentPtr& indexDocument, indexid_t indexId, size_t shardId);

    void SetTemperatureLayer(const std::string& layer);
    void EndSegment();
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<std::unique_ptr<common::DumpItem>>& dumpItems);
    bool IsPrimaryKeyStrValid(const std::string& str) const;

    MultiFieldIndexSegmentReaderPtr CreateInMemSegmentReader() const;

    void GetDumpEstimateFactor(std::priority_queue<double>& factors, std::priority_queue<size_t>& minMemUses) const;
    // TODO: remove
    double GetIndexPoolToDumpFileRatio();
    // TODO: remove
    const std::shared_ptr<framework::SegmentMetrics>& GetSegmentMetrics() const { return _segmentMetrics; }

    void SetBuildProfilingMetrics(const index::BuildProfilingMetricsPtr& metrics) { _profilingMetrics = metrics; }

public:
    static size_t EstimateInitMemUse(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                     const config::IndexPartitionSchemaPtr& schema,
                                     const config::IndexPartitionOptions& options,
                                     const plugin::PluginManagerPtr& pluginManager,
                                     const index_base::PartitionSegmentIteratorPtr& segIter);

    static bool IsTruncateIndex(const config::IndexConfigPtr& indexConfig);

private:
    static constexpr double DEFAULT_INDEX_BYTE_SLICE_POOL_DUMP_FILE_RATIO = 0.2;
    static constexpr double TRIE_INDEX_BYTE_SLICE_POOL_DUMP_FILE_RATIO = 3.5;

private:
    config::IndexSchemaPtr _indexSchema;
    std::vector<std::unique_ptr<index::IndexWriter>> _indexWriters;
    std::map<fieldid_t, std::map<indexid_t, IndexWriter*>> _fieldIdToIndexIdToWriterMap;
    std::map<indexid_t, IndexWriter*> _indexIdToWriterMap;

    std::shared_ptr<framework::SegmentMetrics> _segmentMetrics;
    index::PrimaryKeyWriter* _primaryKeyWriter;
    index::BuildProfilingMetricsPtr _profilingMetrics;

private:
    IE_LOG_DECLARE();
    friend class InMemoryIndexSegmentWriterTest;
};

DEFINE_SHARED_PTR(InMemoryIndexSegmentWriter);
}} // namespace indexlib::index
