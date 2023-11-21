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
#include "indexlib/table/normal_table/NormalTabletWriter.h"

#include <unordered_set>

#include "autil/TimeUtility.h"
#include "future_lite/Future.h"
#include "indexlib/config/BuildOptionConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/document_rewriter/DocumentRewriteChain.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/IndexReaderParameter.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/NormalDocIdDispatcher.h"
#include "indexlib/table/normal_table/NormalDocumentRewriteChainCreator.h"
#include "indexlib/table/normal_table/NormalMemSegment.h"
#include "indexlib/table/normal_table/NormalTabletInfoHolder.h"
#include "indexlib/table/normal_table/NormalTabletModifier.h"
#include "indexlib/table/normal_table/NormalTabletParallelBuilder.h"

using namespace indexlibv2::framework;
using namespace indexlib::config;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletWriter);

NormalTabletWriter::NormalTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema,
                                       const config::TabletOptions* options)
    : CommonTabletWriter(schema, options)
{
}
NormalTabletWriter::~NormalTabletWriter() { Close(); }

void NormalTabletWriter::Close()
{
    if (_parallelBuilder != nullptr) {
        _parallelBuilder->WaitFinish();
    }
    _parallelBuilder = nullptr;
}

Status NormalTabletWriter::Open(const std::shared_ptr<framework::TabletData>& tabletData,
                                const BuildResource& buildResource, const OpenOptions& openOptions)
{
    if (openOptions.GetUpdateControlFlowOnly()) {
        if (_parallelBuilder == nullptr) {
            return Status::InternalError("parallel builder is not intialized");
        }
        AUTIL_LOG(INFO, "switch build mode from [%d] to [%d]", (int)(_parallelBuilder->GetBuildMode()),
                  (int)(openOptions.GetBuildMode()));
        return _parallelBuilder->SwitchBuildMode(openOptions.GetBuildMode());
    }
    auto status = CommonTabletWriter::Open(tabletData, buildResource, openOptions);
    RETURN_IF_STATUS_ERROR(status, "open normal tablet writer failed");

    std::tie(status, _documentRewriteChain) = NormalDocumentRewriteChainCreator::Create(_schema);
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    _versionLocator = tabletData->GetOnDiskVersion().GetLocator();
    _buildTabletData = tabletData;
    if (!_options->IsOnline()) {
        // offline build only care current segment, update doc for other segment will be record in op log
        framework::Version version;
        _buildTabletData.reset(new framework::TabletData(tabletData->GetTabletName()));
        auto status = _buildTabletData->Init(version, {_buildingSegment}, tabletData->GetResourceMap());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "prepare build tablet data failed");
            return status;
        }
    }
    _normalBuildingSegment = std::dynamic_pointer_cast<NormalMemSegment>(_buildingSegment);
    _buildingSegmentBaseDocId = 0;
    auto slice = _buildTabletData->CreateSlice();
    for (const auto& seg : slice) {
        if (seg->GetSegmentStatus() == Segment::SegmentStatus::ST_BUILT ||
            seg->GetSegmentStatus() == Segment::SegmentStatus::ST_DUMPING) {
            _buildingSegmentBaseDocId += seg->GetSegmentInfo()->GetDocCount();
        }
    }

    _parallelBuilder = std::make_unique<indexlib::table::NormalTabletParallelBuilder>();
    RETURN_IF_STATUS_ERROR(_parallelBuilder->Init(_normalBuildingSegment, _options,
                                                  buildResource.consistentModeBuildThreadPool,
                                                  buildResource.inconsistentModeBuildThreadPool),
                           "init parallel builder failed[%s]", status.ToString().c_str());
    AUTIL_LOG(INFO, "switch build mode from [%d] to [%d]", (int)(_parallelBuilder->GetBuildMode()),
              (int)(openOptions.GetBuildMode()));
    RETURN_STATUS_DIRECTLY_IF_ERROR(_parallelBuilder->SwitchBuildMode(openOptions.GetBuildMode()));
    if (openOptions.GetBuildMode() != OpenOptions::BuildMode::STREAM) {
        RETURN_IF_STATUS_ERROR(_parallelBuilder->PrepareForWrite(_schema, _buildTabletData),
                               "prepare parallel builder failed");
    }
    RETURN_IF_STATUS_ERROR(PrepareBuiltinIndex(_buildTabletData), "prepare builtin index failed");
    return Status::OK();
}

void NormalTabletWriter::DispatchDocIds(document::IDocumentBatch* batch)
{
    auto docIdDiapatcher = std::make_unique<indexlib::table::NormalDocIdDispatcher>(
        _schema->GetTableName(), _pkReader, _buildingSegmentBaseDocId,
        /*currentSegmentDocCount=*/_normalBuildingSegment->GetSegmentInfo()->docCount);
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(batch);
    while (iter->HasNext()) {
        auto normalDoc = std::dynamic_pointer_cast<document::NormalDocument>(iter->Next());
        if (normalDoc) {
            docIdDiapatcher->DispatchDocId(normalDoc);
        }
    }
}

Status NormalTabletWriter::PrepareBuiltinIndex(const std::shared_ptr<framework::TabletData>& tabletData)
{
    bool hasPkConfig = !_schema->GetIndexConfigs(index::PRIMARY_KEY_INDEX_TYPE_STR).empty();
    bool hasDeletionMapConfig = !_schema->GetIndexConfigs(index::DELETION_MAP_INDEX_TYPE_STR).empty();
    if (hasPkConfig) {
        RETURN_IF_STATUS_ERROR(PreparePrimaryKeyReader(tabletData), "prepare pk reader failed");
    }
    if (hasPkConfig && hasDeletionMapConfig) {
        RETURN_IF_STATUS_ERROR(PrepareModifier(tabletData), "prepare modifier failed");
    }
    return Status::OK();
}

Status NormalTabletWriter::PreparePrimaryKeyReader(const std::shared_ptr<framework::TabletData>& tabletData)
{
    std::string indexType = index::PRIMARY_KEY_INDEX_TYPE_STR;
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto pkConfigs = _schema->GetIndexConfigs(indexType);
    if (pkConfigs.size() != 1) {
        AUTIL_LOG(ERROR, "invalid pk config size [%zu]", pkConfigs.size());
        return Status::Corruption("invalid pk config size ", pkConfigs.size());
    }
    auto pkConfig = pkConfigs[0];
    auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create index factory for index type [%s] failed", indexType.c_str());
        return status;
    }
    auto indexReader = indexFactory->CreateIndexReader(pkConfig, index::IndexReaderParameter {});
    status = indexReader->Open(pkConfig, tabletData.get());
    if (!status.IsOK()) {
        return status;
    }
    std::shared_ptr<index::IIndexReader> reader = std::move(indexReader);
    _pkReader = std::dynamic_pointer_cast<indexlib::index::PrimaryKeyIndexReader>(reader);
    assert(_pkReader);
    return Status::OK();
}

Status NormalTabletWriter::PrepareModifier(const std::shared_ptr<framework::TabletData>& tabletData)
{
    _modifier.reset(new NormalTabletModifier);
    RETURN_IF_STATUS_ERROR(_modifier->Init(_schema, *tabletData, /*deleteInBranch=*/false, /*op2patchdir*/ nullptr),
                           "prepare tablet modifier failed");
    return Status::OK();
}

std::vector<std::shared_ptr<document::IDocumentBatch>>
NormalTabletWriter::SplitDocumentBatch(document::IDocumentBatch* batch)
{
    std::vector<size_t> splitSizeVec;
    CalculateDocumentBatchSplitSize(batch, splitSizeVec);

    std::vector<std::shared_ptr<document::IDocumentBatch>> subDocBatches;
    subDocBatches.reserve(splitSizeVec.size());

    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(batch);
    for (size_t i = 0; i < splitSizeVec.size(); i++) {
        size_t curBatchSize = splitSizeVec[i];
        if (curBatchSize <= 0) {
            continue;
        }
        auto subDocBatch = std::shared_ptr<document::IDocumentBatch>(batch->Create());
        size_t j = 0;
        while (iter->HasNext() && j < curBatchSize) {
            subDocBatch->AddDocument(iter->Next());
            j++;
        }
        subDocBatches.emplace_back(std::move(subDocBatch));
    }
    return subDocBatches;
}

void NormalTabletWriter::CalculateDocumentBatchSplitSize(document::IDocumentBatch* batch,
                                                         std::vector<size_t>& splitSizeVec)
{
    std::unordered_set<std::string> pkInBatch;
    size_t curBatchSize = 0;
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(batch);
    while (iter->HasNext()) {
        auto normalDoc = std::dynamic_pointer_cast<indexlibv2::document::NormalDocument>(iter->Next());
        const std::string& pkStr = normalDoc->GetPrimaryKey();
        auto iter = pkInBatch.find(pkStr);
        if (iter != pkInBatch.end()) {
            splitSizeVec.push_back(curBatchSize);
            curBatchSize = 0;
            pkInBatch.clear();
        }
        pkInBatch.insert(pkStr);
        curBatchSize++;
    }
    if (curBatchSize > 0) {
        splitSizeVec.push_back(curBatchSize);
    }
    return;
}

void NormalTabletWriter::PostBuildActions()
{
    _tabletData->RefreshDocCount();
    _buildTabletData->RefreshDocCount();
    if (_options->IsOnline()) {
        UpdateNormalTabletInfo();
    }
}

Status NormalTabletWriter::DoBuildAndReportMetrics(const std::shared_ptr<document::IDocumentBatch>& batch)
{
    Status status;
    try {
        ValidateDocumentBatch(batch.get());
        if (_parallelBuilder->GetBuildMode() != OpenOptions::STREAM) {
            DispatchDocIds(batch.get());
            status = RewriteDocumentBatch(batch.get());
            RETURN_IF_STATUS_ERROR(status, "rewrite document batch failed");
            ReportBuildDocumentMetrics(batch.get());
            status = _parallelBuilder->Build(batch);
            RETURN_IF_STATUS_ERROR(status, "parallel build document batch failed");
            PostBuildActions();
        } else {
            auto subDocBatches = SplitDocumentBatch(batch.get());
            for (auto subDocBatch : subDocBatches) {
                DispatchDocIds(subDocBatch.get());
                status = RewriteDocumentBatch(subDocBatch.get());
                RETURN_IF_STATUS_ERROR(status, "rewrite document batch failed");
                status = ModifyDocumentBatch(subDocBatch.get());
                RETURN_IF_STATUS_ERROR(status, "modify document batch failed");
                status = _normalBuildingSegment->Build(subDocBatch.get());
                RETURN_IF_STATUS_ERROR(status, "build document batch failed");
                PostBuildActions();
            }
            ReportBuildDocumentMetrics(batch.get());
        }
    } catch (const indexlib::util::FileIOException& e) {
        auto status = Status::IOError("build caught io exception [%s]", e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    } catch (...) {
        auto status = Status::Unknown("build caught unknown exception");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    return status;
}

void NormalTabletWriter::UpdateNormalTabletInfo()
{
    if (_normalTabletInfoHolder == nullptr) {
        auto resourceMap = _tabletData->GetResourceMap();
        assert(resourceMap);
        auto resource = resourceMap->GetResource(NORMAL_TABLET_INFO_HOLDER);
        if (resource == nullptr) {
            AUTIL_LOG(WARN, "get resource fail, resourceName[%s]", NORMAL_TABLET_INFO_HOLDER.c_str());
            return;
        }
        _normalTabletInfoHolder = std::dynamic_pointer_cast<NormalTabletInfoHolder>(resource);
        assert(_normalTabletInfoHolder);
    }
    auto slice = _tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILDING);
    if (slice.empty()) {
        return;
    }
    const auto& lastRtSegment = *slice.rbegin();
    size_t lastRtSegDocCount = lastRtSegment->GetSegmentInfo()->docCount;
    _normalTabletInfoHolder->UpdateNormalTabletInfo(lastRtSegDocCount);
}

// TODO(yonghao.fyh): unordered doc in batch may cause data consistent problem, add check
void NormalTabletWriter::ValidateDocumentBatch(document::IDocumentBatch* batch)
{
    bool onlyDeleteDoc = true;
    for (size_t i = 0; i < batch->GetBatchSize(); i++) {
        if ((*batch)[i]->GetDocOperateType() != DELETE_DOC) {
            onlyDeleteDoc = false;
        }
        if (_versionLocator.IsSameSrc((*batch)[i]->GetLocatorV2(), true)) {
            auto ret = _versionLocator.IsFasterThan((*batch)[i]->GetDocInfo());
            assert(ret != framework::Locator::LocatorCompareResult::LCR_INVALID);
            if (ret == framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER) {
                batch->DropDoc(i);
            }
        }
    }
    if (!onlyDeleteDoc) {
        _normalBuildingSegment->ValidateDocumentBatch(batch);
    }
    if (_modifier) {
        // TODO(xiaohao.yxh) dedup for ORC table
        return;
    }

    std::unordered_set<std::string> pkSet;
    for (size_t i = 0; i < batch->GetBatchSize(); ++i) {
        if (!batch->IsDropped(i)) {
            auto doc = dynamic_cast<indexlibv2::document::NormalDocument*>((*batch)[i].get());
            assert(doc);
            if (doc->GetDocOperateType() != ADD_DOC) {
                // TODO: yijie.zhang only support add doc
                batch->DropDoc(i);
            }
            if (_pkReader) {
                const std::string& pkStr = doc->GetPrimaryKey();
                auto docid = _pkReader->Lookup(pkStr, /*future_lite::Executor*=*/nullptr);
                if (docid != INVALID_DOCID) {
                    batch->DropDoc(i);
                }
                // TODO: yijie.zhang test same pk in same batch, reconsider filter by set
                if (pkSet.find(pkStr) != pkSet.end()) {
                    batch->DropDoc(i);
                } else {
                    pkSet.insert(pkStr);
                }
            }
        }
    }
}

Status NormalTabletWriter::RewriteDocumentBatch(document::IDocumentBatch* batch)
{
    return _documentRewriteChain->Rewrite(batch);
}

Status NormalTabletWriter::ModifyDocumentBatch(document::IDocumentBatch* batch)
{
    if (!_modifier) {
        // TODO(xiaohao.yxh) dedup for ORC table
        return Status::OK();
    }
    return _modifier->ModifyDocument(batch);
}

std::unique_ptr<SegmentDumper> NormalTabletWriter::CreateSegmentDumper()
{
    if (!IsDirty()) {
        AUTIL_LOG(INFO, "building segment is not dirty, create nullptr segment dumper");
        return nullptr;
    }
    if (_parallelBuilder->GetBuildMode() != OpenOptions::STREAM) {
        _parallelBuilder->WaitFinish();
        PostBuildActions();
    }
    _buildingSegment->Seal();
    return std::make_unique<SegmentDumper>(_tabletData->GetTabletName(), _buildingSegment,
                                           GetBuildingSegmentDumpExpandSize(),
                                           _buildResource.metricsManager->GetMetricsReporter());
}

} // namespace indexlibv2::table
