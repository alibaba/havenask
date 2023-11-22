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
#include "indexlib/table/plain/PlainMemSegment.h"

#include "autil/UnitUtil.h"
#include "beeper/beeper.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/PlainDocument.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/IIndexer.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/table/plain/PlainDumpItem.h"
#include "indexlib/table/plain/SegmentMetricsDumpItem.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/BuildResourceCalculator.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"

using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.table, PlainMemSegment);

#define SEGMENT_LOG(level, format, args...)                                                                            \
    AUTIL_LOG(level, "[%s] [%p] segment[%d] " format, _options->GetTabletName().c_str(), this, GetSegmentId(), ##args)

Status PlainMemSegment::Open(const framework::BuildResource& resource,
                             indexlib::framework::SegmentMetrics* lastSegMetrics)
{
    _currentBuildDocId.reset(new docid64_t(0));
    SEGMENT_LOG(INFO, "begin open mem segment");
    _buildResourceMetrics = resource.buildResourceMetrics;
    _segmentMemController = std::make_unique<MemoryQuotaController>("in_memory_segment", resource.memController);
    _lastSegmentMetrics.reset(new indexlib::framework::SegmentMetrics);
    if (lastSegMetrics) {
        _lastSegmentMetrics->FromString(lastSegMetrics->ToString());
    }
    auto [status, sortDescs] = _schema->GetRuntimeSettings().GetValue<config::SortDescriptions>("sort_descriptions");
    if (status.IsOK()) {
        _sortDescriptions = sortDescs;
    } else if (!status.IsNotFound()) {
        return status;
    }
    status = PrepareIndexers(resource);
    if (!status.IsOK()) {
        Cleanup();
        return status;
    }
    UpdateMemUse();
    return status;
}

void PlainMemSegment::PrepareIndexerParameter(const framework::BuildResource& resource, int64_t maxMemoryUseInBytes,
                                              index::MemIndexerParameter& indexerParam) const
{
    indexerParam.segmentId = GetSegmentId();
    indexerParam.metricsManager = resource.metricsManager;
    indexerParam.buildResourceMetrics = _buildResourceMetrics.get();
    indexerParam.indexMemoryReclaimer = resource.indexMemoryReclaimer;
    indexerParam.maxMemoryUseInBytes = maxMemoryUseInBytes;
    indexerParam.lastSegmentMetrics = _lastSegmentMetrics.get();
    indexerParam.isOnline = _options->IsOnline();
    indexerParam.segmentInfo = GetSegmentInfo();
    indexerParam.segmentMetrics = GetSegmentMetrics();
    indexerParam.isTolerateDocError = _options->GetBuildConfig().GetIsTolerateDocError();
    indexerParam.tabletName = _options->GetTabletName();
    indexerParam.sortDescriptions = _sortDescriptions;
    indexerParam.currentBuildDocId = _currentBuildDocId;
}

Status PlainMemSegment::PrepareIndexers(const framework::BuildResource& resource)
{
    _docInfoExtractorFactory = std::make_unique<plain::DocumentInfoExtractorFactory>();
    if (!_buildResourceMetrics) {
        _buildResourceMetrics = std::make_shared<BuildResourceMetrics>();
        _buildResourceMetrics->Init();
    }
    std::vector<IndexerResource> indexerResources;
    auto status = GenerateIndexerResource(resource, indexerResources);
    RETURN_IF_STATUS_ERROR(status, "generate indexer resource failed for table[%s]", _schema->GetTableName().c_str());
    std::set<std::string> insertedIndexTypes;
    for (auto& indexerResource : indexerResources) {
        auto indexConfig = indexerResource.indexConfig;
        auto indexType = indexConfig->GetIndexType();
        auto indexName = indexConfig->GetIndexName();
        indexerResource.indexerParam.indexerDirectories = std::make_shared<index::IndexerDirectories>(indexConfig);
        for (const auto& [segId, segDir] : resource.segmentDirs) {
            if (segDir) {
                RETURN_IF_STATUS_ERROR(
                    indexerResource.indexerParam.indexerDirectories->AddSegment(segId, segDir->GetIDirectory()),
                    "Add segment dir[%d] to param failed", segId);
            }
        }
        auto indexer = indexerResource.indexerFactory->CreateMemIndexer(indexConfig, indexerResource.indexerParam);
        if (indexer == nullptr) {
            auto status = Status::Corruption("create building index [%s] failed", indexName.c_str());
            SEGMENT_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        status = indexer->Init(indexConfig, _docInfoExtractorFactory.get());
        if (!status.IsOK()) {
            SEGMENT_LOG(ERROR, "init indexer [%s] failed", indexName.c_str());
            return status;
        }
        auto indexMapKey = GenerateIndexMapKey(indexType, indexName);
        auto metricsNode = _buildResourceMetrics->AllocateNode();
        _indexMap[indexMapKey] =
            std::make_pair(indexer, std::make_shared<index::BuildingIndexMemoryUseUpdater>(metricsNode));
        if (insertedIndexTypes.find(indexType) == insertedIndexTypes.end()) {
            _indexMapForValidation[indexMapKey] =
                std::make_pair(indexer, std::make_shared<index::BuildingIndexMemoryUseUpdater>(metricsNode));
            insertedIndexTypes.insert(indexType);
        } else {
            AUTIL_LOG(INFO, "Skipping including index type [%s] for validation", indexType.c_str());
        }
        _indexers.emplace_back(indexer);
        UpdateMemUse();
        if (_segmentMemController->GetFreeQuota() <= 0) {
            SEGMENT_LOG(ERROR, "no mem for open indexer, type[%s], name[%s], free[%ld], allocated[%ld]",
                        indexType.c_str(), indexName.c_str(), _segmentMemController->GetFreeQuota(),
                        _segmentMemController->GetAllocatedQuota());
            return Status::NoMem("segment memory quota exhausted.");
        }
    }
    if (_indexMap.empty()) {
        SEGMENT_LOG(ERROR, "none mem indexer was prepared.");
        return Status::InvalidArgs("empty index map");
    }
    return Status::OK();
}

void PlainMemSegment::Seal()
{
    for (const auto& [_, indexerAndMemUpdater] : _indexMap) {
        const auto& memIndexer = indexerAndMemUpdater.first;
        memIndexer->Seal();
        memIndexer->FillStatistics(_segmentMeta.segmentMetrics);
    }
    UpdateMemUse();
    SEGMENT_LOG(INFO, "seal segment, EvaluateCurrentMemUsed[%s], DocCount[%lu]",
                autil::UnitUtil::GiBDebugString(EvaluateCurrentMemUsed()).c_str(), _segmentMeta.segmentInfo->docCount);
}

std::pair<Status, std::shared_ptr<framework::DumpParams>> PlainMemSegment::CreateDumpParams()
{
    return {Status::OK(), nullptr};
}

std::pair<Status, std::vector<std::shared_ptr<framework::SegmentDumpItem>>> PlainMemSegment::CreateSegmentDumpItems()
{
    auto [st, dumpParams] = CreateDumpParams();
    RETURN2_IF_STATUS_ERROR(st, std::vector<std::shared_ptr<framework::SegmentDumpItem>> {},
                            "create dump params failed");
    std::vector<std::shared_ptr<framework::SegmentDumpItem>> segmentDumpItems;
    std::shared_ptr<autil::mem_pool::PoolBase> dumpPool = std::make_shared<indexlib::util::SimplePool>();
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    for (const auto& [indexMapKey, indexerAndMemUpdater] : _indexMap) {
        auto& indexType = indexMapKey.first;
        auto& memIndexer = indexerAndMemUpdater.first;
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        assert(status.IsOK());
        auto indexDirectory = GetSegmentDirectory()->MakeDirectory(indexFactory->GetIndexPath());
        if (memIndexer->IsDirty()) {
            auto dumpItem = std::make_shared<PlainDumpItem>(dumpPool, memIndexer, indexDirectory, dumpParams);
            segmentDumpItems.push_back(dumpItem);
        }
    }

    auto segMetricsDumpItem =
        std::make_shared<SegmentMetricsDumpItem>(GetSegmentMetrics(), GetSegmentDirectory()->GetIDirectory());
    segmentDumpItems.push_back(segMetricsDumpItem);
    return {Status::OK(), segmentDumpItems};
}

std::pair<Status, std::shared_ptr<index::IIndexer>> PlainMemSegment::GetIndexer(const std::string& type,
                                                                                const std::string& indexName)
{
    auto indexMapKey = GenerateIndexMapKey(type, indexName);
    auto iter = _indexMap.find(indexMapKey);
    if (iter == _indexMap.end()) {
        return std::make_pair(Status::NotFound(), nullptr);
    }
    auto indexerAndMemUpdaterPair = iter->second;
    return std::make_pair(Status::OK(), indexerAndMemUpdaterPair.first);
}

void PlainMemSegment::ValidateDocumentBatch(
    document::TemplateDocumentBatch<document::PlainDocument>* plainDocBatch) const
{
    for (size_t i = 0; i < plainDocBatch->GetBatchSize(); i++) {
        if (plainDocBatch->IsDropped(i)) {
            continue;
        }
        auto plainDoc = plainDocBatch->GetTypedDocument(i).get();
        for (const auto& [indexMapKey, indexerAndMemUpdater] : _indexMap) {
            const auto& indexType = indexMapKey.first;
            auto indexFields = plainDoc->GetIndexFields(indexType);
            if (!indexFields) {
                SEGMENT_LOG(ERROR, "get index fields [%s] failed, drop doc", indexType.c_str());
                plainDocBatch->DropDoc(i);
            }

            auto& memIndexer = indexerAndMemUpdater.first;
            if (!memIndexer->IsValidField(indexFields)) {
                SEGMENT_LOG(ERROR, "mem indexer [%s] check index fields [%s] whether valid failed, drop doc",
                            memIndexer->GetIndexName().c_str(), indexType.c_str());
                plainDocBatch->DropDoc(i);
            }
        }
    }
}
void PlainMemSegment::ValidateDocumentBatch(document::IDocumentBatch* docBatch) const
{
    auto plainDocBatch = dynamic_cast<document::TemplateDocumentBatch<document::PlainDocument>*>(docBatch);
    if (plainDocBatch) {
        ValidateDocumentBatch(plainDocBatch);
    } else {
        for (size_t i = 0; i < docBatch->GetBatchSize(); i++) {
            if (docBatch->IsDropped(i)) {
                continue;
            }
            auto doc = (*docBatch)[i].get();
            for (const auto& [_, indexerAndMemUpdater] : _indexMapForValidation) {
                auto& memIndexer = indexerAndMemUpdater.first;
                if (!memIndexer->IsValidDocument(doc)) {
                    docBatch->DropDoc(i);
                }
            }
        }
    }
}

void PlainMemSegment::PostBuildActions(const indexlibv2::framework::Locator& locator, int64_t maxTimestamp,
                                       int64_t maxTTL, uint64_t addDocCount)
{
    UpdateMemUse();
    _isDirty = true;
    UpdateSegmentInfo(locator, maxTimestamp, maxTTL, addDocCount);
}

Status PlainMemSegment::Build(document::IDocumentBatch* batch)
{
    assert(batch != nullptr);

    auto plainDocBatch = dynamic_cast<document::TemplateDocumentBatch<document::PlainDocument>*>(batch);
    if (plainDocBatch) {
        RETURN_IF_STATUS_ERROR(BuildPlainDoc(plainDocBatch), "build plain doc failed");
    } else {
        // when multi-thread build, build deliver buildThreadPool, memIndexer can parallel build
        const auto& buildIndexers = GetBuildIndexers();
        for (auto& memIndexer : buildIndexers) {
            auto status = memIndexer->Build(batch);
            if (unlikely(!status.IsOK())) {
                UpdateMemUse();
                return status;
            }
        }
    }

    auto lastLocator = batch->GetLastLocator();
    auto maxTimestamp = batch->GetMaxTimestamp();
    auto maxTTL = batch->GetMaxTTL();
    auto addDocCount = batch->GetAddedDocCount();
    PostBuildActions(lastLocator, maxTimestamp, maxTTL, addDocCount);
    return Status::OK();
}
Status PlainMemSegment::BuildPlainDoc(document::TemplateDocumentBatch<document::PlainDocument>* plainDocBatch)
{
    const auto& buildIndexers = GetBuildIndexers();
    auto startDocId = (*_currentBuildDocId);
    for (auto& memIndexer : buildIndexers) {
        *_currentBuildDocId = startDocId;
        auto indexType = memIndexer->GetIndexType();
        std::unique_ptr<indexlibv2::document::DocumentIterator<indexlibv2::document::PlainDocument>> iter =
            document::DocumentIterator<indexlibv2::document::PlainDocument>::Create(plainDocBatch);
        while (iter->HasNext()) {
            auto plainDoc = iter->TypedNext().get();
            auto indexFields = plainDoc->GetIndexFields(indexType);
            if (!indexFields) {
                RETURN_IF_STATUS_ERROR(Status::InternalError(), "get index fields [%s] failed",
                                       indexType.to_string().c_str());
            }
            auto status = memIndexer->Build(indexFields, 1);
            if (unlikely(!status.IsOK())) {
                UpdateMemUse();
                return status;
            }
            (*_currentBuildDocId)++;
        }
    }
    return Status::OK();
}

void PlainMemSegment::UpdateMemUse()
{
    for (const auto& [_, indexerAndMemUpdater] : _indexMap) {
        auto& [memIndexer, memUpdater] = indexerAndMemUpdater;
        memIndexer->UpdateMemUse(memUpdater.get());
    }
    CalcMemCostInCreateDumpParams();

    size_t totalMemUse = indexlib::util::BuildResourceCalculator::GetCurrentTotalMemoryUse(_buildResourceMetrics);
    size_t currentMemUse = _segmentMemController->GetAllocatedQuota();
    int64_t extraMem = totalMemUse - currentMemUse;
    if (extraMem > 0) {
        _segmentMemController->Allocate(extraMem);
    } else if (extraMem < 0) {
        _segmentMemController->Free(-extraMem);
    }
}

void PlainMemSegment::UpdateSegmentInfo(const indexlibv2::framework::Locator& locator, int64_t maxTimestamp,
                                        int64_t maxTTL, uint64_t addDocCount)
{
    if (locator.IsValid()) {
        _segmentMeta.segmentInfo->SetLocator(locator);
    }
    if (_segmentMeta.segmentInfo->timestamp < maxTimestamp) {
        _segmentMeta.segmentInfo->timestamp = maxTimestamp;
    }
    if (_segmentMeta.segmentInfo->maxTTL < maxTTL) {
        _segmentMeta.segmentInfo->maxTTL = maxTTL;
    }
    _segmentMeta.segmentInfo->docCount += addDocCount;
}

bool PlainMemSegment::NeedDump() const
{
    if (_segmentMeta.segmentInfo->docCount >= _options->GetBuildConfig().GetMaxDocCount()) {
        SEGMENT_LOG(INFO, "NeedDump for reach doc count limit : docCount [%lu] over maxDocCount [%lu]",
                    _segmentMeta.segmentInfo->docCount, _options->GetBuildConfig().GetMaxDocCount());
        return true;
    }
    return false;
}

void PlainMemSegment::TEST_AddIndexer(const std::string& type, const std::string& indexName,
                                      const std::shared_ptr<indexlibv2::index::IIndexer> indexer)
{
    auto memIndexer = std::dynamic_pointer_cast<index::IMemIndexer>(indexer);
    if (!memIndexer) {
        return;
    }
    if (!_buildResourceMetrics) {
        _buildResourceMetrics = std::make_shared<BuildResourceMetrics>();
        _buildResourceMetrics->Init();
    }

    auto indexMapKey = GenerateIndexMapKey(type, indexName);
    auto metricsNode = _buildResourceMetrics->AllocateNode();
    _indexMap[indexMapKey] =
        std::make_pair(memIndexer, std::make_shared<index::BuildingIndexMemoryUseUpdater>(metricsNode));
    _indexMapForValidation[indexMapKey] =
        std::make_pair(memIndexer, std::make_shared<index::BuildingIndexMemoryUseUpdater>(metricsNode));
}

const std::vector<std::shared_ptr<indexlibv2::index::IMemIndexer>>& PlainMemSegment::GetBuildIndexers()
{
    return _indexers;
}

Status PlainMemSegment::GenerateIndexerResource(const framework::BuildResource& resource,
                                                std::vector<IndexerResource>& indexerResources)
{
    size_t lastTotalMemUse = 0;
    bool lastMemUseAllFound = true;
    auto indexConfigs = _schema->GetIndexConfigs();
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    for (const auto& indexConfig : indexConfigs) {
        std::string indexType = indexConfig->GetIndexType();
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        if (!status.IsOK()) {
            SEGMENT_LOG(ERROR, "get index factory for index type [%s] failed", indexType.c_str());
            return status;
        }
        IndexerResource indexerResource;
        indexerResource.indexConfig = indexConfig;
        indexerResource.indexerFactory = indexFactory;
        auto [retStatus, memUseSize] = GetLastSegmentMemUse(indexConfig, _lastSegmentMetrics.get());
        if (!retStatus.IsOK() || memUseSize == 0) {
            lastMemUseAllFound = false;
        } else {
            lastTotalMemUse += memUseSize;
            indexerResource.lastMemUseSize = memUseSize;
        }
        indexerResources.emplace_back(std::move(indexerResource));
    }

    for (auto& indexerResource : indexerResources) {
        int64_t maxMemoryUseInBytes = 0;
        if (lastMemUseAllFound && 0 != lastTotalMemUse) {
            maxMemoryUseInBytes = (resource.buildingMemLimit * indexerResource.lastMemUseSize) / lastTotalMemUse;
        } else {
            maxMemoryUseInBytes = resource.buildingMemLimit / indexConfigs.size();
        }
        PrepareIndexerParameter(resource, maxMemoryUseInBytes, indexerResource.indexerParam);
    }
    return Status::OK();
}

} // namespace indexlibv2::plain
