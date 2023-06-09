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
#include "indexlib/index/aggregation/AggregationMemIndexer.h"

#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/index/aggregation/KeyMeta.h"
#include "indexlib/index/aggregation/ReadWriteState.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/CompactPackAttributeDecoder.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueComparator.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AggregationMemIndexer);

AggregationMemIndexer::AggregationMemIndexer(size_t initialKeyCount) : _initialKeyCount(initialKeyCount) {}

AggregationMemIndexer::~AggregationMemIndexer() = default;

Status AggregationMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    _indexConfig = std::dynamic_pointer_cast<config::AggregationIndexConfig>(indexConfig);
    if (!_indexConfig) {
        return Status::InvalidArgs("expect an AggregationIndexConfig");
    }
    _indexHash = config::IndexConfigHash::Hash(indexConfig);

    if (_initialKeyCount == 0) {
        _initialKeyCount = _indexConfig->GetInitialKeyCount();
    }
    AUTIL_LOG(INFO, "initial key count is: %lu", _initialKeyCount);

    auto s = InitValueDecoder();
    if (!s.IsOK()) {
        return s;
    }

    _state = std::make_shared<ReadWriteState>(_initialKeyCount, _indexConfig);
    return Status::OK();
}

Status AggregationMemIndexer::BuildSingleField(const AggregationIndexFields::SingleField& singleField)
{
    auto indexHash = singleField.indexNameHash;
    if (indexHash != 0 && indexHash != _indexHash) {
        return Status::OK();
    }
    auto docType = singleField.opType;
    auto key = singleField.pkeyHash;
    auto value = singleField.value;
    if (docType == ADD_DOC) {
        if (!value.empty()) {
            auto attrMeta = _valueDecoder->Decode(value);
            value = attrMeta.data;
        }
        auto s = _state->postingMap->Add(key, value);
        if (!s.IsOK()) {
            return s;
        }
    } else if (docType == DELETE_DOC && _indexConfig->SupportDelete()) {
        auto s = _state->deleteMap->Add(key, value);
        if (!s.IsOK()) {
            return s;
        }
    } else {
        return Status::InternalError("unsupported doc operation: %d", (int)docType);
    }
    return Status::OK();
}

// deprecated
Status AggregationMemIndexer::Build(document::IDocumentBatch* docBatch)
{
    assert(false);
    return Status::OK();
}

Status AggregationMemIndexer::Build(const document::IIndexFields* indexFields, size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        auto aggregationIndexFields = dynamic_cast<const AggregationIndexFields*>(indexFields + i);
        if (!aggregationIndexFields) {
            return Status::InternalError("not KVIndexFields!");
        }
        for (const auto& singleField : *aggregationIndexFields) {
            RETURN_IF_STATUS_ERROR(BuildSingleField(singleField), "build single field failed ");
        }
    }
    return Status::OK();
}

Status AggregationMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                                   const std::shared_ptr<indexlib::file_system::Directory>& directory,
                                   const std::shared_ptr<framework::DumpParams>& params)
{
    auto idirectory = directory->GetIDirectory();
    auto [s, indexDir] =
        idirectory->MakeDirectory(GetIndexName(), indexlib::file_system::DirectoryOption()).StatusWith();
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "make index directory %s failed, error: %s", GetIndexName().c_str(), s.ToString().c_str());
        return s;
    }
    s = _state->postingMap->Dump(dumpPool, indexDir);
    if (!s.IsOK()) {
        return s;
    }
    if (_indexConfig->SupportDelete()) {
        assert(_state->deleteMap);
        const auto& deleteConfig = _indexConfig->GetDeleteConfig();
        assert(deleteConfig);
        auto [s, deleteDir] =
            indexDir->MakeDirectory(deleteConfig->GetIndexName(), indexlib::file_system::DirectoryOption())
                .StatusWith();
        if (!s.IsOK()) {
            return s;
        }
        return _state->deleteMap->Dump(dumpPool, deleteDir);
    }
    return Status::OK();
}

void AggregationMemIndexer::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        auto doc = (*docBatch)[i].get();
        if (!IsValidDocument(doc)) {
            docBatch->DropDoc(i);
        }
    }
}

bool AggregationMemIndexer::IsValidDocument(document::IDocument* doc)
{
    return doc->GetDocOperateType() == ADD_DOC ||
           (doc->GetDocOperateType() == DELETE_DOC && _indexConfig->SupportDelete());
}

bool AggregationMemIndexer::IsValidField(const document::IIndexFields* fields)
{
    auto aggregationIndexFields = dynamic_cast<const AggregationIndexFields*>(fields);
    if (aggregationIndexFields) {
        return false;
    }
    for (const auto& singleField : *aggregationIndexFields) {
        auto opType = singleField.opType;
        bool isOk = opType == ADD_DOC || (opType == DELETE_DOC && _indexConfig->SupportDelete());
        if (!isOk) {
            return false;
        }
    }
    return true;
}

void AggregationMemIndexer::FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics)
{
    size_t keyCount = _state->postingMap->GetKeyCount();
    if (_state->deleteMap) {
        keyCount = std::max(keyCount, _state->deleteMap->GetKeyCount());
    }
    segmentMetrics->SetDistinctTermCount(GetIndexName(), keyCount);
}

void AggregationMemIndexer::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    int64_t currentMemUse = _state->objPool->getUsedBytes() + _state->bufferPool->getUsedBytes();
    memUpdater->UpdateCurrentMemUse(currentMemUse);

    int64_t dumpTempBufferSize = sizeof(KeyMeta) * _state->postingMap->GetKeyCount();
    if (!_indexConfig->GetSortDescriptions().empty()) {
        dumpTempBufferSize += sizeof(autil::StringView) * _state->postingMap->GetMaxValueCountOfAllKeys();
    }
    memUpdater->EstimateDumpTmpMemUse(dumpTempBufferSize);
    memUpdater->EstimateDumpExpandMemUse(0);
    memUpdater->EstimateDumpedFileSize(_state->postingMap->GetTotalBytes());
}

std::string AggregationMemIndexer::GetIndexName() const { return _indexConfig->GetIndexName(); }
autil::StringView AggregationMemIndexer::GetIndexType() const { return AGGREGATION_INDEX_TYPE_STR; }

void AggregationMemIndexer::Seal() {}

bool AggregationMemIndexer::IsDirty() const { return true; }

std::shared_ptr<ISegmentReader> AggregationMemIndexer::CreateInMemoryReader() const
{
    return _state->CreateInMemoryReader();
}

std::shared_ptr<ISegmentReader> AggregationMemIndexer::CreateInMemoryDeleteReader() const
{
    return _state->CreateDeleteReader();
}

Status AggregationMemIndexer::InitValueDecoder()
{
    const auto& valueConfig = _indexConfig->GetValueConfig();
    auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    if (!status.IsOK() || !packAttrConfig) {
        return Status::ConfigError("create pack attribute config failed for index: %s",
                                   ToJsonString(*_indexConfig, true).c_str());
    }
    auto packAttrConvertor = AttributeConvertorFactory::GetInstance()->CreatePackAttrConvertor(packAttrConfig);
    if (valueConfig->GetFixedLength() != -1) {
        _valueDecoder = std::make_shared<CompactPackAttributeDecoder>(packAttrConvertor);
    } else {
        _valueDecoder.reset(packAttrConvertor);
    }
    return Status::OK();
}

} // namespace indexlibv2::index
