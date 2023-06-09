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
#include "indexlib/index/normal/inverted_index/accessor/in_memory_index_segment_writer.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_multi_sharding_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index/normal/primarykey/primary_key_writer.h"
#include "indexlib/index/util/build_profiling_metrics.h"
#include "indexlib/index/util/dump_item_typed.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace {
class ScopeBuildProfilingReporter
{
public:
    ScopeBuildProfilingReporter(indexlib::index::BuildProfilingMetricsPtr& metric) : _beginTime(0), _metric(metric)
    {
        if (_metric) {
            _beginTime = autil::TimeUtility::currentTime();
        }
    }

    ~ScopeBuildProfilingReporter()
    {
        if (_metric) {
            int64_t endTime = autil::TimeUtility::currentTime();
            _metric->CollectAddIndexTime(endTime - _beginTime);
        }
    }

private:
    int64_t _beginTime;
    indexlib::index::BuildProfilingMetricsPtr& _metric;
};
} // namespace

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InMemoryIndexSegmentWriter);

void InMemoryIndexSegmentWriter::Init(segmentid_t segmentId, const IndexPartitionSchemaPtr& schema,
                                      const IndexPartitionOptions& options,
                                      const std::shared_ptr<indexlib::framework::SegmentMetrics>& lastSegmentMetrics,
                                      util::BuildResourceMetrics* buildResourceMetrics,
                                      const plugin::PluginManagerPtr& pluginManager,
                                      const PartitionSegmentIteratorPtr& segIter)
{
    _segmentMetrics = lastSegmentMetrics;
    _indexSchema = schema->GetIndexSchema();
    assert(_indexSchema);
    _indexWriters.clear();
    auto indexConfigs = _indexSchema->CreateIterator(true);
    auto indexIter = indexConfigs->Begin();
    for (; indexIter != indexConfigs->End(); indexIter++) {
        const IndexConfigPtr& indexConfig = *indexIter;
        if (IsTruncateIndex(indexConfig)) {
            continue;
        }
        if (indexConfig->GetShardingType() == config::IndexConfig::IST_IS_SHARDING) {
            continue;
        }
        std::unique_ptr<IndexWriter> indexWriter = IndexWriterFactory::CreateIndexWriter(
            segmentId, indexConfig, _segmentMetrics, options, buildResourceMetrics, pluginManager, segIter);
        if (indexConfig == _indexSchema->GetPrimaryKeyIndexConfig()) {
            _primaryKeyWriter = static_cast<index::PrimaryKeyWriter*>(indexWriter.get());
        }

        const indexid_t indexId = indexConfig->GetIndexId();
        assert(_indexIdToWriterMap.find(indexId) == _indexIdToWriterMap.end());
        _indexIdToWriterMap[indexId] = indexWriter.get();

        IndexConfig::Iterator iter = indexConfig->CreateIterator();
        while (iter.HasNext()) {
            const FieldConfigPtr& fieldConfig = iter.Next();
            fieldid_t fieldId = fieldConfig->GetFieldId();
            if (_fieldIdToIndexIdToWriterMap.find(fieldId) == _fieldIdToIndexIdToWriterMap.end()) {
                _fieldIdToIndexIdToWriterMap[fieldId] = std::map<indexid_t, IndexWriter*>();
            }
            assert(_fieldIdToIndexIdToWriterMap[fieldId].find(indexId) == _fieldIdToIndexIdToWriterMap[fieldId].end());
            _fieldIdToIndexIdToWriterMap[fieldId][indexId] = indexWriter.get();
        }
        _indexWriters.push_back(std::move(indexWriter));
    }
}

double InMemoryIndexSegmentWriter::GetIndexPoolToDumpFileRatio()
{
    if (_primaryKeyWriter && _primaryKeyWriter->GetIndexConfig()->GetInvertedIndexType() == it_trie) {
        return TRIE_INDEX_BYTE_SLICE_POOL_DUMP_FILE_RATIO;
    }
    return DEFAULT_INDEX_BYTE_SLICE_POOL_DUMP_FILE_RATIO;
}

bool InMemoryIndexSegmentWriter::IsPrimaryKeyStrValid(const std::string& str) const
{
    if (_primaryKeyWriter) {
        return _primaryKeyWriter->CheckPrimaryKeyStr(str);
    }
    return true;
}

size_t InMemoryIndexSegmentWriter::EstimateInitMemUse(
    const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics, const config::IndexPartitionSchemaPtr& schema,
    const config::IndexPartitionOptions& options, const plugin::PluginManagerPtr& pluginManager,
    const index_base::PartitionSegmentIteratorPtr& segIter)
{
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    size_t initMemUse = 0;
    for (; iter != indexConfigs->End(); iter++) {
        IndexConfigPtr indexConfig = *iter;
        if (IsTruncateIndex(indexConfig)) {
            continue;
        }
        // IST_IS_SHARDING index memory will be sumed in IST_NEED_SHARDING.
        if (indexConfig->GetShardingType() == config::IndexConfig::IST_IS_SHARDING) {
            continue;
        }
        initMemUse +=
            IndexWriterFactory::EstimateIndexWriterInitMemUse(indexConfig, options, pluginManager, metrics, segIter);
    }
    return initMemUse;
}

void InMemoryIndexSegmentWriter::AddDocumentByIndex(const document::NormalDocumentPtr& doc, indexid_t indexId,
                                                    size_t shardId)
{
    assert(doc);
    const document::IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    assert(indexDoc);
    assert(_indexIdToWriterMap.find(indexId) != _indexIdToWriterMap.end());
    IndexWriter* indexWriter = _indexIdToWriterMap[indexId];
    const config::IndexConfigPtr& indexConfig = indexWriter->GetIndexConfig();
    config::IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
    if (shardingType == config::IndexConfig::IndexShardingType::IST_NEED_SHARDING) {
        const std::vector<IndexConfigPtr>& shardingIndexConfigs = indexConfig->GetShardingIndexConfigs();
        assert(shardId <= shardingIndexConfigs.size());
        assert(!shardingIndexConfigs.empty());
        if (shardId == shardingIndexConfigs.size()) {
            return;
        }
    }

    config::IndexConfig::Iterator iter = indexConfig->CreateIterator();
    while (iter.HasNext()) {
        const config::FieldConfigPtr& fieldConfig = iter.Next();
        document::Field* field = indexDoc->GetField(fieldConfig->GetFieldId());
        if (field == nullptr) {
            continue;
        }
        assert(shardingType != config::IndexConfig::IndexShardingType::IST_IS_SHARDING);
        if (shardingType == config::IndexConfig::IndexShardingType::IST_NO_SHARDING) {
            indexWriter->AddField(nullptr, field);
        } else {
            auto multiShardingIndexWriter = static_cast<MultiShardingIndexWriter*>(indexWriter);
            multiShardingIndexWriter->AddField(field, shardId);
        }
    }
}

bool InMemoryIndexSegmentWriter::AddDocument(const NormalDocumentPtr& doc)
{
    ScopeBuildProfilingReporter reporter(_profilingMetrics);

    assert(doc);
    const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    assert(indexDoc);
    IndexDocument::FieldVector::const_iterator iter = indexDoc->GetFieldBegin();
    IndexDocument::FieldVector::const_iterator iterEnd = indexDoc->GetFieldEnd();

    for (; iter != iterEnd; ++iter) {
        const Field* field = *iter;
        if (!field) {
            continue;
        }
        fieldid_t fieldId = field->GetFieldId();
        if (_fieldIdToIndexIdToWriterMap.find(fieldId) == _fieldIdToIndexIdToWriterMap.end()) {
            continue;
        }
        for (const auto& pair : _fieldIdToIndexIdToWriterMap[fieldId]) {
            IndexWriter* indexWriter = pair.second;
            indexWriter->AddField(_profilingMetrics, field);
        }
    }
    EndDocument(indexDoc);
    return true;
}

bool InMemoryIndexSegmentWriter::UpdateDocumentByIndex(docid_t docId, const document::NormalDocumentPtr& doc,
                                                       indexid_t indexId, size_t shardId)
{
    assert(doc);
    const document::IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    assert(indexDoc);
    const auto& multiFieldModifiedTokens = indexDoc->GetModifiedTokens();
    if (multiFieldModifiedTokens.empty()) {
        return false;
    }
    bool hasUpdate = false;
    for (const auto& modifiedTokens : multiFieldModifiedTokens) {
        fieldid_t fieldId = modifiedTokens.FieldId();
        if (_fieldIdToIndexIdToWriterMap.find(fieldId) == _fieldIdToIndexIdToWriterMap.end()) {
            continue;
        }
        if (_fieldIdToIndexIdToWriterMap[fieldId].find(indexId) == _fieldIdToIndexIdToWriterMap[fieldId].end()) {
            continue;
        }
        index::IndexWriter* indexWriter = _fieldIdToIndexIdToWriterMap[fieldId][indexId];
        const config::IndexConfigPtr& indexConfig = indexWriter->GetIndexConfig();
        config::IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
        if (not indexConfig->IsIndexUpdatable()) {
            continue;
        }

        if (shardingType == config::IndexConfig::IndexShardingType::IST_NEED_SHARDING) {
            const std::vector<IndexConfigPtr>& shardingIndexConfigs = indexConfig->GetShardingIndexConfigs();
            assert(shardId <= shardingIndexConfigs.size());
            assert(!shardingIndexConfigs.empty());
            if (shardId == shardingIndexConfigs.size()) {
                continue;
            }
            auto multiShardingIndexWriter = static_cast<MultiShardingIndexWriter*>(indexWriter);
            multiShardingIndexWriter->UpdateField(docId, modifiedTokens, shardId);
        } else {
            indexWriter->UpdateField(docId, modifiedTokens);
        }
        hasUpdate = true;
    }
    return hasUpdate;
}

bool InMemoryIndexSegmentWriter::UpdateDocument(docid_t docId, const document::NormalDocumentPtr& doc)
{
    assert(doc);
    const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    assert(indexDoc);
    const auto& multiFieldModifiedTokens = indexDoc->GetModifiedTokens();
    bool hasUpdate = false;
    if (multiFieldModifiedTokens.empty()) {
        return hasUpdate;
    }
    for (const auto& modifiedTokens : multiFieldModifiedTokens) {
        fieldid_t fieldId = modifiedTokens.FieldId();
        if (_fieldIdToIndexIdToWriterMap.find(fieldId) == _fieldIdToIndexIdToWriterMap.end()) {
            continue;
        }
        for (const auto& pair : _fieldIdToIndexIdToWriterMap[fieldId]) {
            IndexWriter* indexWriter = pair.second;
            if (indexWriter->GetIndexConfig()->IsIndexUpdatable()) {
                indexWriter->UpdateField(docId, modifiedTokens);
                hasUpdate = true;
            }
        }
    }
    return hasUpdate;
}

bool InMemoryIndexSegmentWriter::IsTruncateIndex(const IndexConfigPtr& indexConfig)
{
    return !indexConfig->GetNonTruncateIndexName().empty();
}

void InMemoryIndexSegmentWriter::EndDocument(const IndexDocumentPtr& indexDocument)
{
    for (const auto& it : _indexWriters) {
        it->EndDocument(_profilingMetrics, *indexDocument);
    }
}

void InMemoryIndexSegmentWriter::EndDocumentByIndex(const document::IndexDocumentPtr& indexDocument, indexid_t indexId,
                                                    size_t shardId)
{
    assert(_indexIdToWriterMap.find(indexId) != _indexIdToWriterMap.end());
    IndexWriter* indexWriter = _indexIdToWriterMap[indexId];

    config::IndexConfig::IndexShardingType shardingType = indexWriter->GetIndexConfig()->GetShardingType();
    assert(shardingType != config::IndexConfig::IndexShardingType::IST_IS_SHARDING);
    if (shardingType == config::IndexConfig::IndexShardingType::IST_NEED_SHARDING) {
        MultiShardingIndexWriter* multiShardingIndexWriter = static_cast<MultiShardingIndexWriter*>(indexWriter);
        multiShardingIndexWriter->EndDocument(*indexDocument, shardId);
    } else {
        indexWriter->EndDocument(*indexDocument);
    }
}

void InMemoryIndexSegmentWriter::EndSegment()
{
    for (const auto& it : _indexWriters) {
        it->EndSegment();
    }
}

void InMemoryIndexSegmentWriter::SetTemperatureLayer(const string& layer)
{
    for (const auto& it : _indexWriters) {
        it->SetTemperatureLayer(layer);
    }
}

void InMemoryIndexSegmentWriter::CollectSegmentMetrics()
{
    for (const auto& it : _indexWriters) {
        it->FillDistinctTermCount(_segmentMetrics);
    }
}

void InMemoryIndexSegmentWriter::CreateDumpItems(const DirectoryPtr& directory,
                                                 vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    for (const auto& it : _indexWriters) {
        dumpItems.push_back(std::make_unique<IndexDumpItem>(directory, it.get()));
    }
}

MultiFieldIndexSegmentReaderPtr InMemoryIndexSegmentWriter::CreateInMemSegmentReader() const
{
    MultiFieldIndexSegmentReaderPtr indexSegmentReader(new MultiFieldIndexSegmentReader);
    for (uint32_t i = 0; i < _indexWriters.size(); ++i) {
        IndexConfigPtr indexConfig = _indexWriters[i]->GetIndexConfig();
        IndexSegmentReaderPtr indexSegReader = _indexWriters[i]->CreateInMemReader();
        config::IndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
        assert(shardingType == config::IndexConfig::IST_NEED_SHARDING or shardingType == IndexConfig::IST_NO_SHARDING);
        if (shardingType == IndexConfig::IST_NO_SHARDING) {
            indexSegmentReader->AddIndexSegmentReader(indexConfig->GetIndexName(), indexSegReader);
            continue;
        }
        InMemMultiShardingIndexSegmentReaderPtr multiShardingSegReader =
            std::dynamic_pointer_cast<InMemMultiShardingIndexSegmentReader>(indexSegReader);
        assert(multiShardingSegReader != nullptr);
        indexSegmentReader->AddIndexSegmentReader(indexConfig->GetIndexName(), multiShardingSegReader);
        const std::vector<std::pair<std::string, IndexSegmentReaderPtr>>& shardingReaders =
            multiShardingSegReader->GetShardingIndexSegReaders();
        for (size_t i = 0; i < shardingReaders.size(); ++i) {
            indexSegmentReader->AddIndexSegmentReader(shardingReaders[i].first, shardingReaders[i].second);
        }
    }
    return indexSegmentReader;
}

void InMemoryIndexSegmentWriter::GetDumpEstimateFactor(priority_queue<double>& factors,
                                                       priority_queue<size_t>& minMemUses) const
{
    for (size_t i = 0; i < _indexWriters.size(); ++i) {
        _indexWriters[i]->GetDumpEstimateFactor(factors, minMemUses);
    }
}
}} // namespace indexlib::index
