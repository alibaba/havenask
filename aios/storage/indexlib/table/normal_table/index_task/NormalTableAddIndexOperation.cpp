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
#include "indexlib/table/normal_table/index_task/NormalTableAddIndexOperation.h"

#include <limits.h>
#include <unistd.h>

#include "indexlib/analyzer/IAnalyzerFactory.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/NormalDocumentParser.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/table/normal_table/NormalTabletExportLoader.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableAddIndexOperation);

NormalTableAddIndexOperation::NormalTableAddIndexOperation(const framework::IndexOperationDescription& opDesc)
    : framework::IndexOperation(opDesc.GetId(), opDesc.UseOpFenceDir())
    , _opDesc(opDesc)
{
}

NormalTableAddIndexOperation::~NormalTableAddIndexOperation() {}

framework::IndexOperationDescription
NormalTableAddIndexOperation::CreateOperationDescription(framework::IndexOperationId id, schemaid_t targetSchemaId,
                                                         segmentid_t segId,
                                                         std::shared_ptr<config::IIndexConfig> indexConfig)
{
    framework::IndexOperationDescription opDesc(id, NormalTableAddIndexOperation::OPERATION_TYPE);
    opDesc.AddParameter(NormalTableAddIndexOperation::TARGET_SCHEMA_ID, targetSchemaId);
    opDesc.AddParameter(NormalTableAddIndexOperation::TARGET_SEGMENT_ID, segId);
    opDesc.AddParameter(NormalTableAddIndexOperation::TARGET_INDEX_NAME, indexConfig->GetIndexName());
    opDesc.AddParameter(NormalTableAddIndexOperation::TARGET_INDEX_TYPE, indexConfig->GetIndexType());
    return opDesc;
}

Status NormalTableAddIndexOperation::PrepareMemIndexer(const framework::IndexTaskContext& context)
{
    // prepare indexer
    std::string indexerName;
    if (!_opDesc.GetParameter(TARGET_INDEX_NAME, indexerName)) {
        RETURN_STATUS_ERROR(Corruption, "get add index name failed");
    }

    if (!_opDesc.GetParameter(TARGET_SCHEMA_ID, _targetSchemaId)) {
        RETURN_STATUS_ERROR(Corruption, "get target schemaid failed");
    }

    if (!_opDesc.GetParameter(TARGET_SEGMENT_ID, _targetSegmentId)) {
        RETURN_STATUS_ERROR(Corruption, "get target segmentid failed");
    }

    _tabletSchema = context.GetTabletSchema(_targetSchemaId);
    if (!_tabletSchema) {
        RETURN_STATUS_ERROR(Corruption, "can not find target schema [%d]", _targetSchemaId);
    }

    std::string indexerType;
    if (!_opDesc.GetParameter(TARGET_INDEX_TYPE, indexerType)) {
        RETURN_STATUS_ERROR(Corruption, "get add index type failed");
    }

    _indexConfig = _tabletSchema->GetIndexConfig(indexerType, indexerName);
    if (!_indexConfig) {
        RETURN_STATUS_ERROR(Corruption, "get index config [%s] from schema [%d] failed", indexerName.c_str(),
                            _targetSchemaId);
    }

    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = indexFactoryCreator->Create(indexerType);
    RETURN_IF_STATUS_ERROR(status, "get index factory for index type [%s] failed", indexerType.c_str());

    index::IndexerParameter indexerParam;
    indexerParam.metricsManager = context.GetMetricsManager().get();

    auto indexer = indexFactory->CreateMemIndexer(_indexConfig, indexerParam);
    if (indexer == nullptr) {
        RETURN_STATUS_ERROR(Corruption, "create building index [%s] failed", indexerName.c_str());
    }

    plain::DocumentInfoExtractorFactory docInfoExtractorFactory;
    status = indexer->Init(_indexConfig, &docInfoExtractorFactory);
    RETURN_IF_STATUS_ERROR(status, "init indexer [%s] failed", indexerName.c_str());

    _indexer = indexer;
    _segmentDir = context.GetTabletData()->GetOnDiskVersion().GetSegmentDirName(_targetSegmentId);
    _indexDir = indexFactory->GetIndexPath();
    return Status::OK();
}

Status NormalTableAddIndexOperation::PrepareDocIterAndParser(const framework::IndexTaskContext& context)
{
    auto tabletData = context.GetTabletData();
    char cwdPath[PATH_MAX];
    char* ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        assert(false);
        RETURN_STATUS_ERROR(IOError, "get current path failed");
    }
    std::string patchPath =
        indexlib::file_system::FslibWrapper::JoinPath(std::string(cwdPath), std::to_string(_opDesc.GetId()));
    std::set<segmentid_t> targetSegments = {_targetSegmentId};
    indexlib::table::NormalTabletExportLoader loader("" /*fenceName*/, patchPath, targetSegments);
    auto memController = std::make_shared<MemoryQuotaController>("addIndex", 1024);
    auto blockMemoryQuotaController = std::make_shared<indexlib::util::BlockMemoryQuotaController>(memController);
    auto memoryReserver = std::make_shared<indexlib::util::MemoryReserver>("addIndex", blockMemoryQuotaController);
    loader.Init(memController, tabletData->GetOnDiskVersionReadSchema(), memoryReserver, /*isOnline*/ false);
    auto [status, newTabletData] = loader.ProcessTabletData(tabletData);
    RETURN_IF_STATUS_ERROR(status, "export loader process tablet data failed");
    _newTabletData = std::move(newTabletData);

    docid_t baseDocId = 0, endDocId = INVALID_DOCID;
    for (const auto& segment : _newTabletData->CreateSlice()) {
        if (segment->GetSegmentId() == _targetSegmentId) {
            endDocId = baseDocId + segment->GetSegmentInfo()->GetDocCount();
            _targetSegmentDocCount = segment->GetSegmentInfo()->GetDocCount();
            break;
        }
        baseDocId += segment->GetSegmentInfo()->GetDocCount();
    }
    assert(endDocId != INVALID_DOCID);

    std::shared_ptr<framework::ITabletDocIterator::CreateDocIteratorFunc> docIterCreateFunc;
    status = context.GetResourceManager()->GetExtendResource(framework::DOC_READER_ITERATOR_CREATOR, docIterCreateFunc);
    RETURN_IF_STATUS_ERROR(status, "get resource DOC_READER_ITERATOR_CREATOR failed");
    auto iter = (*docIterCreateFunc)(_tabletSchema);

    std::map<std::string, std::string> params;
    std::vector<std::string> fieldNames;
    for (auto fieldConfig : _indexConfig->GetFieldConfigs()) {
        fieldNames.push_back(fieldConfig->GetFieldName());
    }
    std::vector<docid_t> docRange = {baseDocId, endDocId};

    params["__buildin_reserve_deleted_doc__"] = autil::StringUtil::toString(true);
    params["__buildin_docid_range__"] = autil::StringUtil::toString(docRange, "_");

    status = iter->Init(_newTabletData, {100, 100}, context.GetMetricsManager(), fieldNames, params);
    RETURN_IF_STATUS_ERROR(status, "doc iter init failed");
    _docIter = iter;

    std::shared_ptr<analyzer::IAnalyzerFactory> analyzerFactory;
    status = context.GetResourceManager()->GetExtendResource(framework::ANALYZER_FACTORY, analyzerFactory);
    if (!status.IsOK() && !status.IsNotFound()) {
        return status;
    }
    auto parser = std::make_shared<document::NormalDocumentParser>(analyzerFactory, true);
    status = parser->Init(_tabletSchema, nullptr);
    RETURN_IF_STATUS_ERROR(status, "normal table parse init failed");
    _docParser = parser;
    return Status::OK();
}

Status NormalTableAddIndexOperation::Execute(const framework::IndexTaskContext& context)
{
    RETURN_IF_STATUS_ERROR(PrepareMemIndexer(context), "prepare mem indexer failed");
    RETURN_IF_STATUS_ERROR(PrepareDocIterAndParser(context), "prepare doc reader failed");
    docid_t localDocId = 0;
    while (_docIter->HasNext()) {
        std::string ckpt;
        document::IDocument::DocInfo docInfo;
        auto rawDoc = std::make_shared<document::DefaultRawDocument>();
        auto status = _docIter->Next(rawDoc.get(), &ckpt, &docInfo);
        RETURN_IF_STATUS_ERROR(status, "doc iter get next doc failed");
        auto [statusParse, docBatch] = _docParser->ParseRawDoc(rawDoc);
        RETURN_IF_STATUS_ERROR(statusParse, "parse doc failed");
        auto doc = dynamic_cast<document::NormalDocument*>((*docBatch)[0].get());
        assert(doc);
        doc->SetDocId(localDocId);
        status = _indexer->Build(docBatch.get());
        RETURN_IF_STATUS_ERROR(status, "build doc failed");
        localDocId++;
    }
    assert(localDocId == _targetSegmentDocCount);

    auto dumpPool = std::make_shared<indexlib::util::SimplePool>();
    bool useOpFenceDir = _opDesc.UseOpFenceDir();
    auto [status, opFenceDir] = context.GetOpFenceRoot(_opDesc.GetId(), _opDesc.UseOpFenceDir());
    RETURN_IF_STATUS_ERROR(status, "get op fence dir failed");
    auto fenceDir = opFenceDir;
    if (!useOpFenceDir) {
        // legacy code, todo delete by yijie.zhang
        auto removeStatus =
            opFenceDir->RemoveDirectory(_opDesc.GetOpFenceDirName(), indexlib::file_system::RemoveOption::MayNonExist())
                .Status();
        RETURN_IF_STATUS_ERROR(removeStatus, "clear exist op dir failed");
        auto [status, legacyFenceDir] =
            opFenceDir->MakeDirectory(_opDesc.GetOpFenceDirName(), indexlib::file_system::DirectoryOption())
                .StatusWith();
        RETURN_IF_STATUS_ERROR(status, "make op fence dir failed");
        fenceDir = legacyFenceDir;
    }

    auto targetDir = PathUtil::JoinPath(_segmentDir, _indexDir);
    auto [status1, indexDir] =
        fenceDir->MakeDirectory(targetDir, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(status1, "make index dir [%s] failed", _indexDir.c_str());
    auto directory = std::make_shared<indexlib::file_system::Directory>(indexDir);
    _indexer->Seal();
    return _indexer->Dump(dumpPool.get(), directory, nullptr);
}

} // namespace indexlibv2::table
