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
#include "indexlib/index/kv/KVMemIndexerBase.h"

#include "autil/HashAlgorithm.h"
#include "autil/Log.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVTimestamp.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/ValueExtractorUtil.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/util/ValueWriter.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, KVMemIndexerBase);

KVMemIndexerBase::KVMemIndexerBase(bool tolerateDocError) : _tolerateDocError(tolerateDocError), _indexNameHash(0) {}

KVMemIndexerBase::~KVMemIndexerBase() {}

Status KVMemIndexerBase::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                              document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    auto kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    if (!kvIndexConfig) {
        return Status::InvalidArgs("not an kv index config");
    }
    _indexConfig = std::move(kvIndexConfig);

    auto s = ValueExtractorUtil::InitValueExtractor(*_indexConfig, _attrConvertor, _plainFormatEncoder);
    if (!s.IsOK()) {
        return s;
    }

    _indexNameHash = config::IndexConfigHash::Hash(_indexConfig);
    return DoInit();
}

Status KVMemIndexerBase::Build(document::IDocumentBatch* docBatch)
{
    document::KVDocumentBatch* kvDocBatch = dynamic_cast<document::KVDocumentBatch*>(docBatch);
    auto indexNameHash = GetIndexNameHash();
    if (kvDocBatch == nullptr) {
        return Status::InternalError("only support KVDocumentBatch for kv index");
    }
    auto iter = indexlibv2::document::DocumentIterator<document::KVDocument>::Create(kvDocBatch);
    while (iter->HasNext()) {
        auto kvDoc = std::dynamic_pointer_cast<document::KVDocument>(iter->Next());
        auto curIndexNameHash = kvDoc->GetIndexNameHash();
        if (curIndexNameHash != 0 && indexNameHash != curIndexNameHash) {
            continue;
        }
        KVIndexFields::SingleField singleField;
        singleField.pkeyHash = kvDoc->GetPKeyHash();
        singleField.skeyHash = kvDoc->GetSKeyHash();
        singleField.hasSkey = kvDoc->HasSKey();
        singleField.value = kvDoc->GetValue();
        singleField.ttl = kvDoc->GetTTL();
        singleField.userTimestamp = kvDoc->GetUserTimestamp();
        singleField.hasFormatError = kvDoc->HasFormatError();
        singleField.pkFieldName = kvDoc->GetPkFieldName();
        singleField.pkFieldValue = kvDoc->GetPkFieldValue();

        auto s = ConvertBuildDocStatus(BuildSingleField(kvDoc->GetDocOperateType(), singleField));
        if (!s.IsOK()) {
            return s;
        }
    }
    return Status::OK();
}

Status KVMemIndexerBase::Build(const document::IIndexFields* indexFields, size_t n)
{
    auto kvIndexFields = dynamic_cast<const KVIndexFields*>(indexFields);
    if (!kvIndexFields) {
        RETURN_STATUS_ERROR(Status::InvalidArgs, "cast to kv index field failed");
    }
    for (size_t i = 0; i < n; ++i, ++kvIndexFields) {
        auto singleField = kvIndexFields->GetSingleField(GetIndexNameHash());
        if (!singleField) {
            continue;
        }
        RETURN_IF_STATUS_ERROR(
            ConvertBuildDocStatus(BuildSingleField(kvIndexFields->GetDocOperateType(), *singleField)),
            "build single field [%lu] failed", singleField->pkeyHash);
    }
    return Status::OK();
}

Status KVMemIndexerBase::Dump(autil::mem_pool::PoolBase* dumpPool,
                              const std::shared_ptr<indexlib::file_system::Directory>& directory,
                              const std::shared_ptr<framework::DumpParams>& dumpParams)
{
    AUTIL_LOG(INFO, "start dump kv_index[%s]", GetIndexName().c_str());

    auto [s, indexDir] = directory->GetIDirectory()
                             ->MakeDirectory(GetIndexName(), indexlib::file_system::DirectoryOption())
                             .StatusWith();
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "make index directory %s failed, error: %s", GetIndexName().c_str(), s.ToString().c_str());
        return s;
    }

    return DoDump(dumpPool, indexlib::file_system::IDirectory::ToLegacyDirectory(indexDir));
}

void KVMemIndexerBase::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    document::KVDocumentBatch* kvDocBatch = dynamic_cast<document::KVDocumentBatch*>(docBatch);
    if (kvDocBatch == nullptr) {
        AUTIL_LOG(ERROR, "expect a KVDocumentBatch");
        return;
    }
    for (size_t i = 0; i < kvDocBatch->GetBatchSize(); ++i) {
        auto doc = (*kvDocBatch)[i].get();
        auto docType = doc->GetDocOperateType();
        if (docType != ADD_DOC && docType != DELETE_DOC) {
            AUTIL_LOG(WARN, "only support add & delete doc for kv index now");
            kvDocBatch->DropDoc(i);
        }
    }
}
bool KVMemIndexerBase::IsValidDocument(document::IDocument* doc)
{
    auto docType = doc->GetDocOperateType();
    if (docType != ADD_DOC && docType != DELETE_DOC) {
        return false;
    }
    return true;
}

bool KVMemIndexerBase::IsValidField(const document::IIndexFields* fields)
{
    auto kvIndexFields = dynamic_cast<const KVIndexFields*>(fields);
    if (!kvIndexFields) {
        return false;
    }
    auto docType = kvIndexFields->GetDocOperateType();
    if (docType != ADD_DOC && docType != DELETE_DOC) {
        return false;
    }
    return true;
}

void KVMemIndexerBase::FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics)
{
    SegmentStatistics stat;
    DoFillStatistics(stat);
    stat.Store(metrics.get(), GetIndexName());
}

void KVMemIndexerBase::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    MemoryUsage memUsage;
    FillMemoryUsage(memUsage);
    memUpdater->UpdateCurrentMemUse(memUsage.buildMemory);
    memUpdater->EstimateDumpTmpMemUse(memUsage.dumpMemory);
    // value use BufferFileWriter now, so we do not need EstimateDumpedFileSize
}

std::string KVMemIndexerBase::GetIndexName() const { return _indexConfig->GetIndexName(); }
autil::StringView KVMemIndexerBase::GetIndexType() const { return KV_INDEX_TYPE_STR; }
uint64_t KVMemIndexerBase::GetIndexNameHash() const { return _indexNameHash; }
void KVMemIndexerBase::Seal() {}

Status KVMemIndexerBase::BuildSingleField(DocOperateType docType, const KVIndexFields::SingleField& singleField)
{
    if (IsFull()) {
        return Status::NeedDump("IsFull");
    }
    switch (docType) {
    case ADD_DOC:
        return AddField(singleField);
    case DELETE_DOC:
        return DeleteField(singleField);
    default:
        return Status::ParseError("unsupported doc type: ", docType);
    }
}

Status KVMemIndexerBase::AddField(const KVIndexFields::SingleField& field)
{
    auto key = field.pkeyHash;
    uint32_t timestamp;
    if (field.ttl > 0) {
        timestamp = field.ttl;
    } else {
        timestamp = KVTimestamp::Normalize(field.userTimestamp);
    }
    auto value = field.value;
    if (!value.empty()) {
        auto meta = _attrConvertor->Decode(value);
        value = meta.data;
        if (_plainFormatEncoder) {
            _memBuffer.Reserve(value.size());
            if (!_plainFormatEncoder->Encode(value, _memBuffer.GetBuffer(), _memBuffer.GetBufferSize(), value)) {
                return Status::ParseError("encode plain format error.");
            }
        }
    }

    return Add(key, value, timestamp);
}

Status KVMemIndexerBase::DeleteField(const KVIndexFields::SingleField& field)
{
    auto key = field.pkeyHash;
    auto timestamp = KVTimestamp::Normalize(field.userTimestamp);
    return Delete(key, timestamp);
}

Status KVMemIndexerBase::ConvertBuildDocStatus(const Status& status) const
{
    if (!_tolerateDocError || status.IsOK()) {
        return status;
    }
    if (status.IsParseError()) {
        return Status::OK();
    }
    return status;
}

} // namespace indexlibv2::index
