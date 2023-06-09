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
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_writer.h"

#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/index/inverted_index/format/ShardingIndexHasher.h"
#include "indexlib/index/normal/inverted_index/accessor/in_mem_multi_sharding_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_format_writer_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/util/dump_item_typed.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::file_system;

using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MultiShardingIndexWriter);

// 多线程Build时可以指定只处理某个shard的数据，默认处理所有shard的数据
static const size_t ALL_SHARD_ID = (size_t)-1;

MultiShardingIndexWriter::MultiShardingIndexWriter(segmentid_t segmentId,
                                                   const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics,
                                                   const IndexPartitionOptions& options)
    : _segmentId(segmentId)
    , _basePos(0)
    , _segmentMetrics(segmentMetrics)
    , _options(options)
{
}

void MultiShardingIndexWriter::Init(const IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics)
{
    assert(indexConfig);
    if (indexConfig->GetShardingType() != IndexConfig::IST_NEED_SHARDING) {
        INDEXLIB_FATAL_ERROR(BadParameter, "index [%s] should be need_sharding index",
                             indexConfig->GetIndexName().c_str());
    }

    const auto& shardingIndexConfigs = indexConfig->GetShardingIndexConfigs();
    if (shardingIndexConfigs.empty()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "index [%s] has none sharding indexConfigs",
                             indexConfig->GetIndexName().c_str());
    }

    IndexWriter::Init(indexConfig, buildResourceMetrics);
    for (size_t i = 0; i < shardingIndexConfigs.size(); ++i) {
        std::unique_ptr<IndexWriter> indexWriter = IndexWriterFactory::CreateIndexWriter(
            _segmentId, shardingIndexConfigs[i], _segmentMetrics, _options, buildResourceMetrics,
            /*pluginManager=*/nullptr, /*segIter=*/nullptr);
        std::shared_ptr<IndexWriter> sharedIndexWriter(indexWriter.release());
        auto shardingIndexWriter = std::dynamic_pointer_cast<NormalIndexWriter>(sharedIndexWriter);
        if (shardingIndexWriter == nullptr) {
            INDEXLIB_FATAL_ERROR(BadParameter, "CreateShardingIndexWriter for sharding index [%s] failed!",
                                 shardingIndexConfigs[i]->GetIndexName().c_str());
        }
        _shardingWriters.push_back(shardingIndexWriter);
    }
    _sectionAttributeWriter = IndexWriterFactory::CreateSectionAttributeWriter(indexConfig, buildResourceMetrics);
    _shardingIndexHasher = std::make_unique<ShardingIndexHasher>();
    _shardingIndexHasher->Init(_indexConfig);
}

size_t MultiShardingIndexWriter::EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                                                    const index_base::PartitionSegmentIteratorPtr& segIter)
{
    size_t initMemUse = 0;
    assert(indexConfig);
    if (indexConfig->GetShardingType() != IndexConfig::IST_NEED_SHARDING) {
        INDEXLIB_FATAL_ERROR(BadParameter, "index [%s] should be need_sharding index",
                             indexConfig->GetIndexName().c_str());
    }

    const std::vector<IndexConfigPtr>& shardingIndexConfigs = indexConfig->GetShardingIndexConfigs();
    if (shardingIndexConfigs.empty()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "index [%s] has none sharding indexConfigs",
                             indexConfig->GetIndexName().c_str());
    }

    for (size_t i = 0; i < shardingIndexConfigs.size(); ++i) {
        initMemUse += IndexWriterFactory::EstimateIndexWriterInitMemUse(
            shardingIndexConfigs[i], _options, /*pluginManager=*/nullptr, _segmentMetrics, segIter);
    }
    return initMemUse;
}

void MultiShardingIndexWriter::AddField(const Field* field) { return AddField(field, ALL_SHARD_ID); }

void MultiShardingIndexWriter::AddField(const Field* field, size_t shardId)
{
    Field::FieldTag tag = field->GetFieldTag();
    if (tag == Field::FieldTag::NULL_FIELD) {
        size_t shardingIdx = _shardingIndexHasher->GetShardingIdxForNullTerm();
        if (shardId == ALL_SHARD_ID or shardId == shardingIdx) {
            _shardingWriters[shardingIdx]->AddField(field);
        }
        return;
    }

    const auto tokenizeField = dynamic_cast<const IndexTokenizeField*>(field);
    if (!tokenizeField) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "fieldTag [%d] is not IndexTokenizeField",
                             static_cast<int8_t>(field->GetFieldTag()));
    }
    if (tokenizeField->GetSectionCount() <= 0) {
        return;
    }

    pos_t tokenBasePos = _basePos;
    uint32_t fieldLen = 0;
    fieldid_t fieldId = tokenizeField->GetFieldId();

    for (auto iterField = tokenizeField->Begin(); iterField != tokenizeField->End(); ++iterField) {
        const Section* section = *iterField;
        fieldLen += section->GetLength();

        for (size_t i = 0; i < section->GetTokenCount(); i++) {
            const Token* token = section->GetToken(i);
            tokenBasePos += token->GetPosIncrement();
            AddToken(token, fieldId, tokenBasePos, shardId);
        }
    }
    _basePos += fieldLen;
}

void MultiShardingIndexWriter::UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    return UpdateField(docId, modifiedTokens, ALL_SHARD_ID);
}

void MultiShardingIndexWriter::UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens,
                                           size_t shardId)
{
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        auto token = modifiedTokens[i];
        index::DictKeyInfo termKey(token.first);
        UpdateField(docId, termKey, token.second == document::ModifiedTokens::Operation::REMOVE, shardId);
    }
    document::ModifiedTokens::Operation op;
    if (modifiedTokens.GetNullTermOperation(&op)) {
        index::DictKeyInfo termKey = index::DictKeyInfo::NULL_TERM;
        UpdateField(docId, termKey, op == document::ModifiedTokens::Operation::REMOVE, shardId);
    }
}

void MultiShardingIndexWriter::UpdateField(docid_t docId, index::DictKeyInfo termKey, bool isDelete)
{
    return UpdateField(docId, termKey, isDelete, ALL_SHARD_ID);
}

void MultiShardingIndexWriter::UpdateField(docid_t docId, index::DictKeyInfo termKey, bool isDelete, size_t shardId)
{
    size_t shardingIdx = _shardingIndexHasher->GetShardingIdx(termKey);
    if (shardId == ALL_SHARD_ID or shardId == shardingIdx) {
        _shardingWriters[shardingIdx]->UpdateField(docId, termKey, isDelete);
    }
}

void MultiShardingIndexWriter::EndDocument(const IndexDocument& indexDocument)
{
    for (size_t i = 0; i < _shardingWriters.size(); ++i) {
        _shardingWriters[i]->EndDocument(indexDocument);
    }

    if (_sectionAttributeWriter) {
        _sectionAttributeWriter->EndDocument(indexDocument);
    }
    _basePos = 0;
}

void MultiShardingIndexWriter::EndDocument(const IndexDocument& indexDocument, size_t shardId)
{
    assert(shardId <= _shardingWriters.size());
    if (shardId < _shardingWriters.size()) {
        _shardingWriters[shardId]->EndDocument(indexDocument);
    } else {
        if (_sectionAttributeWriter) {
            _sectionAttributeWriter->EndDocument(indexDocument);
        }
    }
    _basePos = 0;
}

void MultiShardingIndexWriter::EndSegment()
{
    for (size_t i = 0; i < _shardingWriters.size(); ++i) {
        _shardingWriters[i]->EndSegment();
    }
}

void MultiShardingIndexWriter::Dump(const DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool)
{
    for (size_t i = 0; i < _shardingWriters.size(); i++) {
        _shardingWriters[i]->Dump(dir, dumpPool);
    }

    if (_sectionAttributeWriter) {
        _sectionAttributeWriter->Dump(dir, dumpPool);
    }
}

void MultiShardingIndexWriter::CreateDumpItems(const DirectoryPtr& directory,
                                               vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    for (size_t i = 0; i < _shardingWriters.size(); ++i) {
        dumpItems.push_back(std::make_unique<IndexDumpItem>(directory, _shardingWriters[i].get()));
    }

    if (_sectionAttributeWriter) {
        dumpItems.push_back(std::make_unique<IndexDumpItem>(directory, _sectionAttributeWriter.get()));
    }
}

void MultiShardingIndexWriter::FillDistinctTermCount(
    std::shared_ptr<indexlib::framework::SegmentMetrics> _segmentMetrics)
{
    for (size_t i = 0; i < _shardingWriters.size(); ++i) {
        _shardingWriters[i]->FillDistinctTermCount(_segmentMetrics);
    }
}

uint32_t MultiShardingIndexWriter::GetDistinctTermCount() const
{
    assert(false);
    return 0;
}

uint64_t MultiShardingIndexWriter::GetNormalTermDfSum() const
{
    uint64_t termDfSum = 0;
    for (size_t i = 0; i < _shardingWriters.size(); ++i) {
        termDfSum += _shardingWriters[i]->GetNormalTermDfSum();
    }
    return termDfSum;
}

IndexSegmentReaderPtr MultiShardingIndexWriter::CreateInMemReader()
{
    std::vector<std::pair<std::string, IndexSegmentReaderPtr>> segReaders;
    for (size_t i = 0; i < _shardingWriters.size(); ++i) {
        const string& indexName = _shardingWriters[i]->GetIndexConfig()->GetIndexName();
        segReaders.push_back(make_pair(indexName, _shardingWriters[i]->CreateInMemReader()));
    }

    AttributeSegmentReaderPtr sectionReader;
    if (_sectionAttributeWriter) {
        sectionReader = _sectionAttributeWriter->CreateInMemAttributeReader();
    }

    InMemMultiShardingIndexSegmentReaderPtr inMemSegReader(
        new InMemMultiShardingIndexSegmentReader(segReaders, sectionReader));
    return inMemSegReader;
}

void MultiShardingIndexWriter::GetDumpEstimateFactor(priority_queue<double>& factors,
                                                     priority_queue<size_t>& minMemUses) const
{
    for (size_t i = 0; i < _shardingWriters.size(); ++i) {
        _shardingWriters[i]->GetDumpEstimateFactor(factors, minMemUses);
    }

    if (_sectionAttributeWriter) {
        _sectionAttributeWriter->GetDumpEstimateFactor(factors, minMemUses);
    }
}

void MultiShardingIndexWriter::AddToken(const Token* token, fieldid_t fieldId, pos_t tokenBasePos)
{
    return AddToken(token, fieldId, tokenBasePos, ALL_SHARD_ID);
}

void MultiShardingIndexWriter::AddToken(const Token* token, fieldid_t fieldId, pos_t tokenBasePos, size_t shardId)
{
    assert(_shardingIndexHasher);
    size_t shardingIdx = _shardingIndexHasher->GetShardingIdx(token);
    if (shardId == ALL_SHARD_ID or shardId == shardingIdx) {
        _shardingWriters[shardingIdx]->AddToken(token, fieldId, tokenBasePos);
    }
}

IndexWriter* MultiShardingIndexWriter::GetIndexWriter(uint32_t shardId) const
{
    assert(shardId <= _shardingWriters.size());
    if (shardId == _shardingWriters.size()) {
        return _sectionAttributeWriter.get();
    }
    return _shardingWriters[shardId].get();
}
}} // namespace indexlib::index
