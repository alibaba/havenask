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
#include "indexlib/index/source/SourceMemIndexer.h"

#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/SerializedSourceDocument.h"
#include "indexlib/document/normal/SourceFormatter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/common/GroupFieldDataWriter.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/SourceGroupWriter.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using indexlib::document::SerializedSourceDocument;
namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SourceMemIndexer);

SourceMemIndexer::SourceMemIndexer(const indexlibv2::index::MemIndexerParameter& indexerParam)
    : _buildResourceMetrics(indexerParam.buildResourceMetrics)
{
}

Status SourceMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                              document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    AUTIL_LOG(INFO, "begin init source writer");
    _config = std::dynamic_pointer_cast<config::SourceIndexConfig>(indexConfig);
    if (!_config) {
        RETURN_STATUS_ERROR(Corruption, "get source index config failed");
    }
    if (_config->IsDisabled()) {
        RETURN_STATUS_ERROR(InvalidArgs, "all source field group disabled");
    }
    for (const auto& groupConfig : _config->GetGroupConfigs()) {
        if (groupConfig->IsDisabled()) {
            _dataWriters.push_back({nullptr, nullptr});
            AUTIL_LOG(INFO, "group [%d] is disabled, ignore", groupConfig->GetGroupId());
            continue;
        }
        auto groupWriter = std::make_shared<SourceGroupWriter>();
        groupWriter->Init(groupConfig);

        std::shared_ptr<indexlibv2::index::BuildingIndexMemoryUseUpdater> memUpdater = nullptr;
        if (_buildResourceMetrics != nullptr) { // buildResourceMetrics might be nullptr in UT.
            auto metricsNode = _buildResourceMetrics->AllocateNode();
            memUpdater = std::make_shared<indexlibv2::index::BuildingIndexMemoryUseUpdater>(metricsNode);
        }
        _dataWriters.push_back({groupWriter, memUpdater});
    }
    std::shared_ptr<config::FileCompressConfigV2> compressConfig;
    _metaWriter.reset(new GroupFieldDataWriter(compressConfig));
    _metaWriter->Init(SOURCE_DATA_FILE_NAME, SOURCE_OFFSET_FILE_NAME, VarLenDataParamHelper::MakeParamForSourceMeta());
    std::any emptyFieldHint;
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, document::extractor::DocumentInfoExtractorType::SOURCE_DOC, emptyFieldHint);
    if (!_docInfoExtractor) {
        RETURN_STATUS_ERROR(Corruption, "create doc info extractor failed");
    }
    AUTIL_LOG(INFO, "end init source writer");
    return Status::OK();
}

Status SourceMemIndexer::AddDocument(document::IDocument* doc)
{
    if (_dataWriters.empty()) {
        return Status::OK();
    }
    indexlib::document::SerializedSourceDocument* sourceDoc = nullptr;
    RETURN_STATUS_DIRECTLY_IF_ERROR(_docInfoExtractor->ExtractField(doc, (void**)&sourceDoc));
    const autil::StringView meta = sourceDoc->GetMeta();
    _metaWriter->AddDocument(meta);
    for (size_t groupId = 0; groupId < _dataWriters.size(); groupId++) {
        if (auto groupWriter = _dataWriters[groupId].first) {
            const autil::StringView groupValue = sourceDoc->GetGroupValue(groupId);
            groupWriter->AddDocument(groupValue);
        }
    }
    return Status::OK();
}

Status SourceMemIndexer::Build(document::IDocumentBatch* docBatch)
{
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch);
    while (iter->HasNext()) {
        std::shared_ptr<document::IDocument> doc = iter->Next();
        if (doc->GetDocOperateType() != ADD_DOC) {
            continue;
        }
        RETURN_STATUS_DIRECTLY_IF_ERROR(AddDocument(doc.get()));
    }
    return Status::OK();
}
Status SourceMemIndexer::Build(const document::IIndexFields* indexFields, size_t n)
{
    assert(false);
    return Status::Unimplement();
}
Status SourceMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                              const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                              const std::shared_ptr<framework::DumpParams>& params)
{
    auto dir = indexDirectory->GetIDirectory();
    assert(!_dataWriters.empty());
    AUTIL_LOG(INFO, "begin dump source meta");
    auto [metaSt, metaDir] = dir->MakeDirectory(SOURCE_META_DIR, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(metaSt, "make dir for source meta failed");
    RETURN_IF_STATUS_ERROR(_metaWriter->Dump(metaDir, dumpPool, params), "dump meta failed");
    AUTIL_LOG(INFO, "dump source meta end");
    for (size_t groupId = 0; groupId < _dataWriters.size(); groupId++) {
        if (auto groupWriter = _dataWriters[groupId].first) {
            auto [groupSt, groupDir] =
                dir->MakeDirectory(GetDataDir(groupId), indexlib::file_system::DirectoryOption()).StatusWith();
            RETURN_IF_STATUS_ERROR(groupSt, "make directory for group failed");
            RETURN_IF_STATUS_ERROR(groupWriter->Dump(groupDir, dumpPool, params), "dump data failed");
            AUTIL_LOG(INFO, "dump source group[%lu] end", groupId);
        }
    }
    return Status::OK();
}
void SourceMemIndexer::ValidateDocumentBatch(document::IDocumentBatch* docBatch) { assert(false); }
bool SourceMemIndexer::IsValidDocument(document::IDocument* doc) { return _docInfoExtractor->IsValidDocument(doc); }
bool SourceMemIndexer::IsValidField(const document::IIndexFields* fields)
{
    assert(false);
    return false;
}
void SourceMemIndexer::FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) {}
void SourceMemIndexer::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    if (_metaWriter) {
        _metaWriter->UpdateMemUse(memUpdater);
    }

    for (const auto& [dataWriter, groupMemUpdater] : _dataWriters) {
        if (dataWriter) {
            assert(groupMemUpdater);
            dataWriter->UpdateMemUse(groupMemUpdater.get());
        }
    }
}
std::string SourceMemIndexer::GetIndexName() const { return SOURCE_INDEX_NAME; }
autil::StringView SourceMemIndexer::GetIndexType() const { return SOURCE_INDEX_TYPE_STR; }
void SourceMemIndexer::Seal() {}
bool SourceMemIndexer::IsDirty() const { return _metaWriter ? (_metaWriter->GetNumDocuments() > 0) : false; }

bool SourceMemIndexer::GetDocument(docid_t localDocId, const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                                   indexlib::document::SerializedSourceDocument* serDoc) const
{
    auto metaAccessor = _metaWriter->GetDataAccessor();
    if (!metaAccessor) {
        assert(false);
        AUTIL_LOG(ERROR, "meta accessor is nullptr");
        return false;
    }
    if ((uint64_t)localDocId >= metaAccessor->GetDocCount()) {
        return false;
    }

    uint8_t* metaValue = NULL;
    uint32_t metaSize = 0;
    if (!metaAccessor->ReadData(localDocId, metaValue, metaSize)) {
        return false;
    }
    if (metaSize == 0) {
        return false;
    }
    autil::StringView meta((char*)metaValue, metaSize);
    serDoc->SetMeta(meta);

    for (auto groupId : requiredGroupdIds) {
        auto groupReader = _dataWriters[groupId].first;
        if (!groupReader) {
            autil::StringView groupData = autil::StringView::empty_instance();
            serDoc->SetGroupValue(groupId, groupData);
            continue;
        }
        uint8_t* value = NULL;
        uint32_t size = 0;
        auto groupAccessor = groupReader->GetDataAccessor();
        if (!groupAccessor) {
            assert(false);
            AUTIL_LOG(ERROR, "group accessor is nullptr");
            return false;
        }
        if (!groupAccessor->ReadData(localDocId, value, size)) {
            return false;
        }
        if (size == 0) {
            return false;
        }
        autil::StringView groupValue((char*)value, size);
        serDoc->SetGroupValue(groupId, groupValue);
    }
    return true;
}

} // namespace indexlibv2::index
