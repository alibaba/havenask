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
#include "indexlib/index/field_meta/FieldMetaMemIndexer.h"

#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/extractor/plain/FieldMetaFieldInfoExtractor.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/field_meta/meta/MetaFactory.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, FieldMetaMemIndexer);

Status
FieldMetaMemIndexer::Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                          indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    _fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    assert(_fieldMetaConfig);

    auto fieldConfigs = _fieldMetaConfig->GetFieldConfigs();
    assert(1u == fieldConfigs.size());
    auto fieldId = std::make_any<fieldid_t>(fieldConfigs[0]->GetFieldId());
    assert(docInfoExtractorFactory);
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, indexlibv2::document::extractor::DocumentInfoExtractorType::FIELD_META_FIELD, fieldId);
    auto [status, fieldMetas] = MetaFactory::CreateFieldMetas(_fieldMetaConfig);
    RETURN_IF_STATUS_ERROR(status, "create field meta failed");
    _fieldMetas = fieldMetas;
    assert(!_fieldMetas.empty());
    _convertor.reset(new FieldMetaConvertor());
    if (!_convertor->Init(_fieldMetaConfig)) {
        AUTIL_LOG(ERROR, "field meta [%s] convertor init failed", _fieldMetaConfig->GetIndexName().c_str());
        return Status::Corruption();
    }
    const auto sourceType = _fieldMetaConfig->GetStoreMetaSourceType();
    switch (sourceType) {
    case FieldMetaConfig::MetaSourceType::MST_FIELD:
        _sourceWriter =
            std::make_shared<SourceFieldWriter<FieldMetaConfig::MetaSourceType::MST_FIELD>>(docInfoExtractorFactory);
        break;
    case FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT:
        _sourceWriter = std::make_shared<SourceFieldWriter<FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT>>(
            docInfoExtractorFactory);
        break;
    case FieldMetaConfig::MetaSourceType::MST_NONE:
        _sourceWriter =
            std::make_shared<SourceFieldWriter<FieldMetaConfig::MetaSourceType::MST_NONE>>(docInfoExtractorFactory);
        break;
    default:
        assert(false);
    }

    RETURN_IF_STATUS_ERROR(_sourceWriter->Init(_fieldMetaConfig), "source writer init failed for meta[%s]",
                           _fieldMetaConfig->GetIndexName().c_str());
    return Status::OK();
}

Status FieldMetaMemIndexer::Build(indexlibv2::document::IDocumentBatch* docBatch)
{
    IFieldMeta::FieldValueBatch batch;
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch);
    while (iter->HasNext()) {
        std::shared_ptr<indexlibv2::document::IDocument> doc = iter->Next();
        auto docId = doc->GetDocId();
        if (doc->GetDocOperateType() != ADD_DOC) {
            AUTIL_LOG(DEBUG, "doc[%d] isn't add_doc", docId);
            continue;
        }
        _isDirty = true;
        std::pair<autil::StringView, bool> fieldPair;
        auto pointer = &fieldPair;
        RETURN_STATUS_DIRECTLY_IF_ERROR(_docInfoExtractor->ExtractField(doc.get(), (void**)(&pointer)));
        auto [fieldEncodedValue, isNull] = fieldPair;
        FieldValueMeta fieldValueMeta;
        bool ret = false;
        if (likely(!isNull)) {
            std::tie(ret, fieldValueMeta) = _convertor->Decode(fieldEncodedValue);
            if (!ret) {
                return Status::Corruption("field meta convertor decode failed");
            }
        } else {
            fieldValueMeta.fieldValue = _fieldMetaConfig->GetFieldConfig()->GetNullFieldLiteralString();
        }
        batch.push_back({fieldValueMeta, isNull, docId});
    }
    for (auto& fieldMeta : _fieldMetas) {
        auto status = fieldMeta->Build(batch);
        RETURN_IF_STATUS_ERROR(status, "build field infos failed");
    }
    auto status = _sourceWriter->Build(batch);
    RETURN_IF_STATUS_ERROR(status, "build source field meta failed");
    return Status::OK();
}

Status FieldMetaMemIndexer::Build(const indexlibv2::document::IIndexFields* indexFields, size_t n)
{
    assert(false);
    return Status::Corruption("not support");
}
void FieldMetaMemIndexer::ValidateDocumentBatch(indexlibv2::document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        if (!docBatch->IsDropped(i)) {
            if (!IsValidDocument((*docBatch)[i].get())) {
                docBatch->DropDoc(i);
            }
        }
    }
}
bool FieldMetaMemIndexer::IsValidDocument(indexlibv2::document::IDocument* doc)
{
    return _docInfoExtractor->IsValidDocument(doc);
}

bool FieldMetaMemIndexer::IsValidField(const indexlibv2::document::IIndexFields* fields)
{
    assert(false);
    return false;
}

std::string FieldMetaMemIndexer::GetIndexName() const { return _fieldMetaConfig->GetIndexName(); }

Status FieldMetaMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                                 const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                                 const std::shared_ptr<indexlibv2::framework::DumpParams>& params)
{
    std::string fieldMetaName = _fieldMetaConfig->GetIndexName();
    AUTIL_LOG(DEBUG, "Dumping fieldMeta : [%s]", fieldMetaName.c_str());
    if (!IsDirty()) {
        return Status::OK();
    }

    auto [st, subDir] = indexDirectory->GetIDirectory()
                            ->MakeDirectory(fieldMetaName, indexlib::file_system::DirectoryOption())
                            .StatusWith();
    RETURN_IF_STATUS_ERROR(st, "make subdir [%s] fail, ErrorInfo: [%s].", fieldMetaName.c_str(), st.ToString().c_str());

    for (auto& fieldMeta : _fieldMetas) {
        auto status = fieldMeta->Store(dumpPool, subDir);
        RETURN_IF_STATUS_ERROR(status, "field meta [%s] dump failed", fieldMeta->GetFieldMetaName().c_str());
    }

    auto status = _sourceWriter->Dump(dumpPool, subDir, params);
    RETURN_IF_STATUS_ERROR(status, "source writer dump failed");
    return Status::OK();
}

std::shared_ptr<IFieldMeta> FieldMetaMemIndexer::GetSegmentFieldMeta(const std::string& fieldMetaType) const
{
    for (const auto& fieldMeta : _fieldMetas) {
        if (fieldMeta->GetFieldMetaName() == fieldMetaType) {
            return fieldMeta;
        }
    }
    AUTIL_LOG(ERROR, "fieldMetaType [%s] not found", fieldMetaType.c_str());
    return nullptr;
}

bool FieldMetaMemIndexer::GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount)
{
    return _sourceWriter->GetFieldTokenCount(key, pool, fieldTokenCount);
}

void FieldMetaMemIndexer::UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater)
{
    int64_t currentMemUse = 0, dumpTmpMemUse = 0, dumpExpandMemUse = 0, dumpFileSize = 0;
    int64_t totalCurrentMemUse = 0, totalDumpTmpMemUse = 0, totalDumpExpandMemUse = 0, totalDumpFileSize = 0;
    auto updateGlobalInfo = [&]() {
        totalCurrentMemUse += currentMemUse;
        totalDumpTmpMemUse += std::max(totalDumpTmpMemUse, dumpTmpMemUse);
        totalDumpExpandMemUse += dumpExpandMemUse;
        totalDumpFileSize += dumpFileSize;
    };

    for (const auto& fieldMeta : _fieldMetas) {
        fieldMeta->GetMemUse(currentMemUse, dumpTmpMemUse, dumpExpandMemUse, dumpFileSize);
        updateGlobalInfo();
    }
    _sourceWriter->UpdateMemUse(memUpdater);
    memUpdater->GetMemInfo(currentMemUse, dumpTmpMemUse, dumpExpandMemUse, dumpFileSize);
    updateGlobalInfo();

    memUpdater->UpdateCurrentMemUse(totalCurrentMemUse);
    memUpdater->EstimateDumpTmpMemUse(totalDumpTmpMemUse);
    memUpdater->EstimateDumpExpandMemUse(totalDumpTmpMemUse);
    memUpdater->EstimateDumpedFileSize(totalDumpFileSize);
}

bool FieldMetaMemIndexer::IsDirty() const { return _isDirty; }

} // namespace indexlib::index
