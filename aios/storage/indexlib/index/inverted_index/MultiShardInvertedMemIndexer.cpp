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
#include "indexlib/index/inverted_index/MultiShardInvertedMemIndexer.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/Field.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/document/normal/Section.h"
#include "indexlib/document/normal/Token.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/inverted_index/InvertedMemIndexer.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"
#include "indexlib/index/inverted_index/format/ShardingIndexHasher.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeMemIndexer.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
using indexlibv2::document::IDocument;
using indexlibv2::document::IDocumentBatch;
using indexlibv2::document::IIndexFields;
using indexlibv2::document::extractor::IDocumentInfoExtractorFactory;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, MultiShardInvertedMemIndexer);

MultiShardInvertedMemIndexer::MultiShardInvertedMemIndexer(const indexlibv2::index::MemIndexerParameter& indexerParam)
    : _indexerParam(indexerParam)
{
    // TODO: add indexer dirs to sharding index
    _indexerParam.indexerDirectories.reset();
}

MultiShardInvertedMemIndexer::~MultiShardInvertedMemIndexer() {}

Status MultiShardInvertedMemIndexer::Init(const std::shared_ptr<IIndexConfig>& indexConfig,
                                          IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    std::any emptyFieldHint;
    assert(docInfoExtractorFactory);
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, indexlibv2::document::extractor::DocumentInfoExtractorType::INVERTED_INDEX_DOC,
        /*fieldHint=*/emptyFieldHint);
    if (_docInfoExtractor == nullptr) {
        AUTIL_LOG(ERROR, "create document info extractor failed");
        return Status::InternalError("create document info extractor failed.");
    }
    _indexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    if (_indexConfig == nullptr) {
        AUTIL_LOG(ERROR,
                  "IndexType[%s] IndexName[%s] invalid index config, "
                  "dynamic cast to legacy index config failed.",
                  indexConfig->GetIndexType().c_str(), indexConfig->GetIndexName().c_str());
        return Status::InvalidArgs("invalid index config, dynamic cast to legacy index config failed.");
    }

    if (_indexConfig->GetShardingType() != InvertedIndexConfig::IST_NEED_SHARDING) {
        auto status =
            Status::InvalidArgs("index [%s] should be need_sharding index", _indexConfig->GetIndexName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    const auto& shardIndexConfigs = _indexConfig->GetShardingIndexConfigs();
    if (shardIndexConfigs.empty()) {
        auto status =
            Status::InvalidArgs("index [%s] has none sharding index configs", _indexConfig->GetIndexName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    InvertedIndexConfig::Iterator iter = _indexConfig->CreateIterator();
    while (iter.HasNext()) {
        const auto& fieldConfig = iter.Next();
        fieldid_t fieldId = fieldConfig->GetFieldId();
        _fieldIds.push_back(fieldId);
    }
    if (_fieldIds.empty()) {
        assert(false);
        auto status = Status::InvalidArgs("IndexType[%s] IndexName[%s] invalid index config, has no field id.",
                                          _indexConfig->GetIndexType().c_str(), _indexConfig->GetIndexName().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();

    for (const auto& indexConfig : shardIndexConfigs) {
        std::string indexType = indexConfig->GetIndexType();
        std::string indexName = indexConfig->GetIndexName();
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "get index factory for index type [%s] failed", indexType.c_str());
            return status;
        }
        auto indexer = indexFactory->CreateMemIndexer(indexConfig, _indexerParam);
        if (indexer == nullptr) {
            AUTIL_LOG(ERROR, "create building index [%s] failed", indexName.c_str());
            return Status::Corruption("create building index [%s] failed", indexName.c_str());
        }
        status = indexer->Init(indexConfig, docInfoExtractorFactory);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "init indexer [%s] failed", indexName.c_str());
            return status;
        }
        auto shardIndexer = std::dynamic_pointer_cast<InvertedMemIndexer>(indexer);
        if (shardIndexer == nullptr) {
            AUTIL_LOG(ERROR, "CreateShardingIndexWriter for sharding index [%s] failed!", indexName.c_str());
            return Status::InvalidArgs("CreateShardingIndexWriter for sharding index [%s] failed!", indexName.c_str());
        }
        std::shared_ptr<indexlibv2::index::BuildingIndexMemoryUseUpdater> memUpdater = nullptr;
        if (_indexerParam.buildResourceMetrics != nullptr) { // buildResourceMetrics might be nullptr in UT.
            auto metricsNode = _indexerParam.buildResourceMetrics->AllocateNode();
            memUpdater = std::make_shared<indexlibv2::index::BuildingIndexMemoryUseUpdater>(metricsNode);
        }
        _memIndexers.emplace_back(shardIndexer, memUpdater);
    }

    _indexFormatOption = std::make_shared<IndexFormatOption>();
    _indexFormatOption->Init(_indexConfig);

    if (_indexFormatOption->HasSectionAttribute()) {
        _sectionAttributeMemIndexer = std::make_unique<SectionAttributeMemIndexer>(_indexerParam);
        auto status = _sectionAttributeMemIndexer->Init(_indexConfig, docInfoExtractorFactory);
        RETURN_IF_STATUS_ERROR(status, "section attribute mem indexer init failed, indexName[%s]",
                               GetIndexName().c_str());
    }

    _indexHasher = std::make_unique<ShardingIndexHasher>();
    _indexHasher->Init(_indexConfig);
    return Status::OK();
}

bool MultiShardInvertedMemIndexer::IsDirty() const { return _docCount > 0; }

Status MultiShardInvertedMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                                          const std::shared_ptr<file_system::Directory>& indexDirectory,
                                          const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams)
{
    for (size_t i = 0; i < _memIndexers.size(); i++) {
        auto status = _memIndexers[i].first->Dump(dumpPool, indexDirectory, dumpParams);
        RETURN_IF_STATUS_ERROR(status, "sharded inverted mem indexer dump failed, indexName[%s]",
                               _memIndexers[i].first->GetIndexName().c_str());
    }

    if (_sectionAttributeMemIndexer) {
        auto status = _sectionAttributeMemIndexer->Dump(dumpPool, indexDirectory, dumpParams);
        RETURN_IF_STATUS_ERROR(status, "section mem indexer dump failed, indexName[%s]",
                               _sectionAttributeMemIndexer->GetIndexName().c_str());
    }
    return Status::OK();
}
Status MultiShardInvertedMemIndexer::Build(const IIndexFields* indexFields, size_t n)
{
    assert(0);
    return {};
}
Status MultiShardInvertedMemIndexer::Build(IDocumentBatch* docBatch)
{
    std::vector<document::IndexDocument*> docs;
    auto status = DocBatchToDocs(docBatch, &docs);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "convert doc batch to docs failed");
        return status;
    }
    for (const auto& doc : docs) {
        auto status = AddDocument(doc, INVALID_SHARDID);
        RETURN_IF_STATUS_ERROR(status, "add document failed");
    }
    return Status::OK();
}

Status MultiShardInvertedMemIndexer::AddDocument(document::IndexDocument* doc)
{
    return AddDocument(doc, INVALID_SHARDID);
}

Status MultiShardInvertedMemIndexer::DocBatchToDocs(IDocumentBatch* docBatch,
                                                    std::vector<document::IndexDocument*>* docs) const
{
    docs->reserve(docBatch->GetBatchSize());
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch);
    while (iter->HasNext()) {
        std::shared_ptr<indexlibv2::document::IDocument> doc = iter->Next();
        if (doc->GetDocOperateType() != ADD_DOC) {
            continue;
        }
        document::IndexDocument* indexDoc = nullptr;
        Status s = _docInfoExtractor->ExtractField(doc.get(), (void**)&indexDoc);
        if (!s.IsOK()) {
            return s;
        }
        (*docs).emplace_back(indexDoc);
    }
    return Status::OK();
}

Status MultiShardInvertedMemIndexer::AddDocument(document::IndexDocument* doc, size_t shardId)
{
    pos_t basePos = 0;
    for (auto fieldId : _fieldIds) {
        const document::Field* field = doc->GetField(fieldId);
        auto status = AddField(field, shardId, &basePos);
        RETURN_IF_STATUS_ERROR(status, "add field fail, fieldId[%d]", fieldId);
    }
    EndDocument(*doc, shardId);
    _docCount++;
    return Status::OK();
}

Status MultiShardInvertedMemIndexer::AddField(const document::Field* field, size_t shardId, pos_t* basePos)
{
    if (!field) {
        return Status::OK();
    }
    document::Field::FieldTag tag = field->GetFieldTag();
    if (tag == document::Field::FieldTag::NULL_FIELD) {
        size_t termShardIdx = _indexHasher->GetShardingIdxForNullTerm();
        if (shardId == INVALID_SHARDID || termShardIdx == shardId) {
            return _memIndexers[termShardIdx].first->AddField(field);
        }
        return Status::OK();
    }

    assert(tag == document::Field::FieldTag::TOKEN_FIELD);
    auto tokenizeField = dynamic_cast<const document::IndexTokenizeField*>(field);
    if (!tokenizeField) {
        AUTIL_LOG(ERROR, "fieldTag [%d] is not IndexTokenizeField", static_cast<int8_t>(field->GetFieldTag()));
        return Status::ConfigError("field tag is not IndexTokenizeField");
    }

    if (tokenizeField->GetSectionCount() <= 0) {
        return Status::OK();
    }

    pos_t tokenBasePos = *basePos;
    uint32_t fieldLen = 0;
    fieldid_t fieldId = tokenizeField->GetFieldId();

    for (auto iterField = tokenizeField->Begin(); iterField != tokenizeField->End(); ++iterField) {
        const document::Section* section = *iterField;
        fieldLen += section->GetLength();

        for (size_t i = 0; i < section->GetTokenCount(); i++) {
            const document::Token* token = section->GetToken(i);
            tokenBasePos += token->GetPosIncrement();
            auto status = AddToken(token, fieldId, tokenBasePos, shardId);
            RETURN_IF_STATUS_ERROR(status, "add token failed, fieldId[%d]", fieldId);
        }
    }
    *basePos += fieldLen;
    return Status::OK();
}

Status MultiShardInvertedMemIndexer::AddToken(const document::Token* token, fieldid_t fieldId, pos_t tokenBasePos,
                                              size_t shardId)
{
    assert(_indexHasher);
    size_t termShardId = _indexHasher->GetShardingIdx(token);
    if (shardId == INVALID_SHARDID || termShardId == shardId) {
        return _memIndexers[termShardId].first->AddToken(token, fieldId, tokenBasePos);
    }
    return Status::OK();
}

void MultiShardInvertedMemIndexer::EndDocument(const document::IndexDocument& indexDocument)
{
    EndDocument(indexDocument, INVALID_SHARDID);
}

void MultiShardInvertedMemIndexer::EndDocument(const document::IndexDocument& indexDocument, size_t shardId)
{
    if (shardId != INVALID_SHARDID) {
        if (shardId < _memIndexers.size()) {
            _memIndexers[shardId].first->EndDocument(indexDocument);
        } else {
            assert(shardId == _memIndexers.size());
            if (_sectionAttributeMemIndexer) {
                _sectionAttributeMemIndexer->EndDocument(indexDocument);
            }
        }
        return;
    }

    for (size_t i = 0; i < _memIndexers.size(); ++i) {
        _memIndexers[i].first->EndDocument(indexDocument);
    }

    if (_sectionAttributeMemIndexer) {
        _sectionAttributeMemIndexer->EndDocument(indexDocument);
    }
}

void MultiShardInvertedMemIndexer::ValidateDocumentBatch(IDocumentBatch* docBatch)
{
    for (const auto& indexer : _memIndexers) {
        indexer.first->ValidateDocumentBatch(docBatch);
    }
}
bool MultiShardInvertedMemIndexer::IsValidDocument(IDocument* doc) { return true; }
bool MultiShardInvertedMemIndexer::IsValidField(const IIndexFields* fields) { return true; }
void MultiShardInvertedMemIndexer::UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* updater)
{
    for (const auto& indexer : _memIndexers) {
        auto& memIndexer = indexer.first;
        auto& memUpdater = indexer.second;
        if (memUpdater != nullptr) {
            memIndexer->UpdateMemUse(memUpdater.get());
        }
    }
    if (_sectionAttributeMemIndexer) {
        _sectionAttributeMemIndexer->UpdateMemUse(updater);
    }
}

void MultiShardInvertedMemIndexer::FillStatistics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics)
{
    for (const auto& indexer : _memIndexers) {
        indexer.first->FillStatistics(segmentMetrics);
    }
}

std::string MultiShardInvertedMemIndexer::GetIndexName() const { return _indexConfig->GetIndexName(); }
autil::StringView MultiShardInvertedMemIndexer::GetIndexType() const { return INVERTED_INDEX_TYPE_STR; }

void MultiShardInvertedMemIndexer::Seal()
{
    // do nothing
}

std::shared_ptr<InvertedMemIndexer>
MultiShardInvertedMemIndexer::GetSingleShardIndexer(const std::string& indexName) const
{
    for (const auto& indexer : _memIndexers) {
        if (indexer.first->GetIndexName() == indexName) {
            return indexer.first;
        }
    }
    AUTIL_LOG(WARN, "shard mem indexer [%s] not found", indexName.c_str());
    return nullptr;
}

std::shared_ptr<indexlibv2::index::AttributeMemReader>
MultiShardInvertedMemIndexer::CreateSectionAttributeMemReader() const
{
    std::shared_ptr<indexlibv2::index::AttributeMemReader> sectionReader;
    if (_sectionAttributeMemIndexer) {
        sectionReader = _sectionAttributeMemIndexer->CreateInMemReader();
    }
    return sectionReader;
}

void MultiShardInvertedMemIndexer::UpdateTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    UpdateTokens(docId, modifiedTokens, INVALID_SHARDID);
}

void MultiShardInvertedMemIndexer::UpdateTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens,
                                                size_t shardId)
{
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        auto [termKey, op] = modifiedTokens[i];
        DictKeyInfo dictKeyInfo(termKey);
        size_t termShardId = _indexHasher->GetShardingIdx(dictKeyInfo);
        if (shardId == INVALID_SHARDID || termShardId == shardId) {
            _memIndexers[termShardId].first->UpdateOneTerm(docId, dictKeyInfo, op);
        }
    }
    document::ModifiedTokens::Operation op;
    if (modifiedTokens.GetNullTermOperation(&op)) {
        size_t termShardId = _indexHasher->GetShardingIdx(DictKeyInfo::NULL_TERM);
        if (shardId == INVALID_SHARDID || termShardId == shardId) {
            _memIndexers[termShardId].first->UpdateOneTerm(docId, DictKeyInfo::NULL_TERM, op);
        }
    }
}

std::shared_ptr<InvertedIndexMetrics> MultiShardInvertedMemIndexer::GetMetrics() const
{
    if (_memIndexers.empty()) {
        return nullptr;
    }
    return _memIndexers[0].first->GetMetrics();
}

indexlibv2::document::extractor::IDocumentInfoExtractor* MultiShardInvertedMemIndexer::GetDocInfoExtractor() const
{
    return _docInfoExtractor.get();
}

} // namespace indexlib::index
