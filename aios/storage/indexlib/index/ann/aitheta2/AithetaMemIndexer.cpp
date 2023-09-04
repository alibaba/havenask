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
#include "indexlib/index/ann/aitheta2/AithetaMemIndexer.h"

#include "indexlib/base/Constant.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/DocMapDumpParams.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexConfig.h"
#include "indexlib/index/ann/aitheta2/impl/CustomizedAithetaLogger.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegmentBuilder.h"
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegmentBuilder.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
#include "indexlib/index/ann/aitheta2/util/EmbeddingFieldData.h"
#include "indexlib/index/ann/aitheta2/util/IndexFieldParser.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib;
using namespace indexlib::file_system;

namespace indexlibv2::index::ann {
AUTIL_LOG_SETUP(indexlib.index, AithetaMemIndexer);

AithetaMemIndexer::AithetaMemIndexer(const IndexerParameter& indexerParam) : _indexerParam(indexerParam) {}

AithetaMemIndexer::~AithetaMemIndexer() {}

Status AithetaMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                               document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    CustomizedAiThetaLogger::RegisterIndexLogger();
    std::shared_ptr<indexlibv2::config::ANNIndexConfig> annIndexConfig =
        std::dynamic_pointer_cast<indexlibv2::config::ANNIndexConfig>(indexConfig);
    if (nullptr != indexConfig && nullptr == annIndexConfig) {
        RETURN_STATUS_ERROR(InvalidArgs, "index [%s] cast to ANNIndexConfig failed",
                            indexConfig->GetIndexName().c_str());
    }
    _indexName = annIndexConfig->GetIndexName();
    _aithetaIndexConfig = AithetaIndexConfig(annIndexConfig->GetParameters());
    _fieldParser = std::make_unique<IndexFieldParser>();
    if (!_fieldParser->Init(annIndexConfig)) {
        RETURN_STATUS_ERROR(InvalidArgs, "init field parser failed.");
    }

    assert(docInfoExtractorFactory);
    std::any emptyFieldHint;
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, document::extractor::DocumentInfoExtractorType::INVERTED_INDEX_DOC, /*fieldHint=*/emptyFieldHint);
    if (_docInfoExtractor == nullptr) {
        RETURN_STATUS_ERROR(InternalError, "create document info extractor failed.");
    }

    if (nullptr != _indexerParam.metricsManager) {
        auto kmonReporter = _indexerParam.metricsManager->GetMetricsReporter();
        if (nullptr != kmonReporter) {
            auto provider = std::make_shared<indexlib::util::MetricProvider>(kmonReporter);
            _metricReporter = std::make_shared<MetricReporter>(provider, _indexName);
            _buildLatencyMetric = _metricReporter->Declare("indexlib.vector.build_latency", kmonitor::GAUGE);
            _dumpLatencyMetric = _metricReporter->Declare("indexlib.vector.dump_latency", kmonitor::GAUGE);
        }
    }

    if (_indexerParam.isOnline) {
        return InitRealtimeSegmentBuilder();
    } else {
        if (IsFullBuild()) {
            return InitNormalSegmentBuilder(/*isFullBuildPhase=*/true);
        } else {
            return InitNormalSegmentBuilder(/*isFullBuildPhase=*/false);
        }
    }
}

bool AithetaMemIndexer::ShouldSkipBuild() const
{
    return _indexerParam.isOnline && !_aithetaIndexConfig.realtimeConfig.enable;
}
Status AithetaMemIndexer::Build(const document::IIndexFields* indexFields, size_t n)
{
    assert(0);
    return {};
}
Status AithetaMemIndexer::Build(document::IDocumentBatch* docBatch)
{
    if (ShouldSkipBuild()) {
        AUTIL_LOG(DEBUG, "not enable realtime build.");
        return Status::OK();
    }
    ScopedLatencyReporter reporter(_buildLatencyMetric);
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(docBatch);
    while (iter->HasNext()) {
        indexlibv2::document::IDocument* doc = iter->Next().get();
        if (doc->GetDocOperateType() != ADD_DOC) {
            continue;
        }
        RETURN_STATUS_DIRECTLY_IF_ERROR(BuildSingleDoc(doc));
    }
    return Status::OK();
}

Status AithetaMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool,
                               const std::shared_ptr<indexlib::file_system::Directory>& directory,
                               const std::shared_ptr<framework::DumpParams>& dumpParams)
{
    if (directory->IsExist(GetIndexName())) {
        AUTIL_LOG(WARN, "directory [%s] already exist, will be removed.", GetIndexName().c_str());
        directory->RemoveDirectory(GetIndexName());
    }
    std::shared_ptr<file_system::Directory> aithetaIndexDir = directory->MakeDirectory(GetIndexName());
    if (_indexerParam.isOnline && !_aithetaIndexConfig.realtimeConfig.enable) {
        AUTIL_LOG(WARN, "ann index [%s] not enable realtime build", _indexName.c_str());
        return Status::OK();
    }
    ScopedLatencyReporter reporter(_dumpLatencyMetric);

    const std::vector<docid_t>* old2NewDocId = nullptr;
    if (nullptr != dumpParams) {
        auto docMapDumpParams = std::dynamic_pointer_cast<indexlibv2::index::DocMapDumpParams>(dumpParams);
        old2NewDocId = docMapDumpParams ? &docMapDumpParams->old2new : nullptr;
    }

    if (!_segmentBuilder->Dump(aithetaIndexDir, old2NewDocId)) {
        RETURN_STATUS_ERROR(ConfigError, "aitheta indexer dump index failed");
    }
    AUTIL_LOG(INFO, "Indexer[%p] with index_name [%s] dump doc count [%d] and invalid doc count [%d]", this,
              _indexName.c_str(), _docCount, _invalidDocCount);
    AUTIL_LOG(INFO, "Indexer[%p] with index_name [%s] dump index to directory[%s] success", this, _indexName.c_str(),
              directory->DebugString().c_str());
    return Status::OK();
}

bool AithetaMemIndexer::IsValidDocument(document::IDocument* doc) { return _docInfoExtractor->IsValidDocument(doc); }
bool AithetaMemIndexer::IsValidField(const document::IIndexFields* fields)
{
    assert(0);
    return false;
}

void AithetaMemIndexer::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); i++) {
        if (!docBatch->IsDropped(i)) {
            if (!_docInfoExtractor->IsValidDocument((*docBatch)[i].get())) {
                docBatch->DropDoc(i);
            }
        }
    }
}

std::pair<Status, std::shared_ptr<RealtimeSegmentSearcher>>
AithetaMemIndexer::CreateSearcher(docid_t segmentBaseDocId, const std::shared_ptr<AithetaFilterCreator>& creator)
{
    if (!_indexerParam.isOnline || !_aithetaIndexConfig.realtimeConfig.enable) {
        AUTIL_SLOG(ERROR) << "_indexerParam.isOnline is [" << _indexerParam.isOnline << "]; enable realtime is ["
                          << _aithetaIndexConfig.realtimeConfig.enable << "]";
        return {Status::OK(), nullptr};
    }
    auto realtimeBuilderPtr = std::dynamic_pointer_cast<RealtimeSegmentBuilder>(_segmentBuilder);
    if (nullptr == realtimeBuilderPtr) {
        AUTIL_LOG(ERROR, "cast to RealtimeSegmentBuilder failed, index name [%s]", _indexName.c_str());
        return {Status::InternalError("cast to RealtimeSegmentBuilder failed."), nullptr};
    }
    auto realtimeSearcherPtr = std::make_shared<RealtimeSegmentSearcher>(_aithetaIndexConfig, MetricReporterPtr());
    if (!realtimeSearcherPtr->Init(realtimeBuilderPtr->GetRealtimeSegment(), segmentBaseDocId, creator)) {
        AUTIL_LOG(ERROR, "init RealtimeSegmentSearcher failed, index name [%s]", _indexName.c_str());
        return {Status::InternalError("init RealtimeSegmentSearcher failed."), nullptr};
    }
    return {Status::OK(), realtimeSearcherPtr};
}

void AithetaMemIndexer::FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics)
{
    // TODO(peaker.lgf): need to impl
}

void AithetaMemIndexer::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    if (_indexerParam.isOnline && !_aithetaIndexConfig.realtimeConfig.enable) {
        return;
    }
    int64_t currentMemUse = _segmentBuilder->EstimateCurrentMemoryUse();
    int64_t dumpTempMemUse = _segmentBuilder->EstimateTempMemoryUseInDump();
    int64_t dumpFileSize = _segmentBuilder->EstimateDumpFileSize();

    memUpdater->UpdateCurrentMemUse(currentMemUse);
    memUpdater->EstimateDumpTmpMemUse(dumpTempMemUse);
    memUpdater->EstimateDumpedFileSize(dumpFileSize);
}

void AithetaMemIndexer::Seal()
{
    // TODO(peaker.lgf): need to impl
}

bool AithetaMemIndexer::IsDirty() const
{
    // IsDirty() means whether current segment constains docs;
    // if there is no doc in segment, Dump(...) will not be called;
    // but in AithetaIndex, empty dir must be created even though segment is empty;
    return true;
}

bool AithetaMemIndexer::IsFullBuild()
{
    assert(nullptr != _indexerParam.indexerDirectories);
    auto iter = _indexerParam.indexerDirectories->CreateIndexerDirectoryIterator();
    auto segmentDirsPair = iter.Next();
    while (segmentDirsPair.first != INVALID_SEGMENTID) {
        for (const auto& dir : segmentDirsPair.second) {
            SegmentMeta segmentMeta;
            if (segmentMeta.Load(make_shared<Directory>(dir)) && segmentMeta.IsMergedSegment()) {
                return false;
            }
        }
        segmentDirsPair = iter.Next();
    }
    return true;
}

std::shared_ptr<RealtimeSegmentBuildResource> AithetaMemIndexer::CreateRealtimeBuildResource()
{
    vector<std::shared_ptr<Directory>> indexerDirs;
    if (nullptr == _indexerParam.indexerDirectories) {
        return std::make_shared<RealtimeSegmentBuildResource>(_aithetaIndexConfig, indexerDirs);
    }
    auto iter = _indexerParam.indexerDirectories->CreateIndexerDirectoryIterator();
    auto indexerDirsPair = iter.Next();
    while (indexerDirsPair.first != INVALID_SEGMENTID) {
        for (const auto& indexerDir : indexerDirsPair.second) {
            indexerDirs.push_back(std::make_shared<Directory>(indexerDir));
        }
        indexerDirsPair = iter.Next();
    }
    return std::make_shared<RealtimeSegmentBuildResource>(_aithetaIndexConfig, indexerDirs);
}

Status AithetaMemIndexer::InitRealtimeSegmentBuilder()
{
    if (_indexerParam.isOnline && !_aithetaIndexConfig.realtimeConfig.enable) {
        return Status::OK();
    }
    auto resource = CreateRealtimeBuildResource();
    _segmentBuilder = make_shared<RealtimeSegmentBuilder>(_aithetaIndexConfig, _indexName, _metricReporter);

    // NOT hold resource, it may be removed by framework later
    if (!_segmentBuilder->Init(resource)) {
        return Status::InternalError("init realtime segment builder failed.");
    }
    return Status::OK();
}

Status AithetaMemIndexer::InitNormalSegmentBuilder(bool isFullBuildPhase)
{
    AithetaIndexConfig aithetaIndexConfig = _aithetaIndexConfig;
    if (isFullBuildPhase && !_aithetaIndexConfig.buildConfig.buildInFullBuildPhase) {
        // not build index in full-builder except that it is o2o.
        aithetaIndexConfig.searchConfig.searcherName = LINEAR_SEARCHER;
        aithetaIndexConfig.buildConfig.builderName = LINEAR_BUILDER;
    }

    _segmentBuilder = make_shared<NormalSegmentBuilder>(aithetaIndexConfig, _indexName, false, _metricReporter);
    if (!_segmentBuilder->Init(SegmentBuildResourcePtr())) {
        return Status::InternalError("init realtime segment builder failed.");
    }
    return Status::OK();
}

Status AithetaMemIndexer::BuildSingleDoc(document::IDocument* doc)
{
    indexlib::document::IndexDocument* indexDoc = nullptr;
    Status s = _docInfoExtractor->ExtractField(doc, (void**)&indexDoc);
    RETURN_STATUS_DIRECTLY_IF_ERROR(s);

    EmbeddingFieldData fieldData;
    IndexFieldParser::ParseStatus parseStatus = _fieldParser->Parse(indexDoc, fieldData);
    Status status = Status::OK();
    if (IndexFieldParser::ParseStatus::PS_OK == parseStatus) {
        if (!_segmentBuilder->Build(fieldData)) {
            status = Status::InternalError("build document failed");
        }
    } else if (IndexFieldParser::ParseStatus::PS_FAIL == parseStatus) {
        status = Status::InternalError("parse document fields failed");
    }

    if (status.IsOK()) {
        _docCount++;
    } else if (_aithetaIndexConfig.buildConfig.ignoreBuildError) {
        _invalidDocCount++;
        status = Status::OK();
    }

    return status;
}

} // namespace indexlibv2::index::ann
