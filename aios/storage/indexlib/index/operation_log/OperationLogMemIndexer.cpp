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
#include "indexlib/index/operation_log/OperationLogMemIndexer.h"

#include "autil/Lock.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentDumpItem.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/operation_log/Common.h"
#include "indexlib/index/operation_log/InMemSegmentOperationIterator.h"
#include "indexlib/index/operation_log/NormalSegmentOperationIterator.h"
#include "indexlib/index/operation_log/OperationBlock.h"
#include "indexlib/index/operation_log/OperationDumper.h"
#include "indexlib/index/operation_log/OperationFactory.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"
#include "indexlib/index/operation_log/OperationLogMetrics.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationLogMemIndexer);

OperationLogMemIndexer::OperationLogMemIndexer(segmentid_t segmentid) : _segmentid(segmentid)
{
    _opBlocks.reset(new OperationBlockVec);
}

OperationLogMemIndexer::~OperationLogMemIndexer()
{
    if (_metrics) {
        _metrics->Stop();
    }
}

Status
OperationLogMemIndexer::Init(const std::shared_ptr<IIndexConfig>& indexConfig,
                             indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    _opConfig = std::dynamic_pointer_cast<OperationLogConfig>(indexConfig);
    _operationFieldInfo = std::make_shared<OperationFieldInfo>();
    _operationFieldInfo->Init(_opConfig);
    assert(_opConfig);
    _maxBlockSize = _opConfig->GetMaxBlockSize();
    _operationFactory = CreateOperationFactory(_opConfig);
    RETURN_IF_STATUS_ERROR(CreateNewBlock(_maxBlockSize), "create new block failed");
    return Status::OK();
}
Status OperationLogMemIndexer::Build(const indexlibv2::document::IIndexFields* indexFields, size_t n)
{
    assert(0);
    return Status::OK();
}
Status OperationLogMemIndexer::Build(indexlibv2::document::IDocumentBatch* docBatch)
{
    assert(docBatch);
    assert(_opConfig->GetPrimaryKeyIndexConfig());
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        // doc is dropped.
        if (docBatch->IsDropped(i)) {
            continue;
        }
        std::shared_ptr<indexlibv2::document::IDocument> doc = (*docBatch)[i];
        RETURN_STATUS_DIRECTLY_IF_ERROR(ProcessDocument(doc.get()));
    }
    return Status::OK();
}

Status OperationLogMemIndexer::ProcessDocument(indexlibv2::document::IDocument* doc)
{
    auto normalDocument = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);

    if (!normalDocument->HasPrimaryKey()) {
        AUTIL_LOG(ERROR, "Doc has no primary key when schema has primary key index.");
        return Status::InvalidArgs("Doc has no primary key when schema has primary key index.");
    }
    {
        autil::ScopedLock lock(_dataLock);
        OperationBase* op = nullptr;
        if (!_operationFactory->CreateOperation(normalDocument, _blockInfo.GetPool(), &op)) {
            AUTIL_LOG(ERROR, "Create operation failed.");
            return Status::InternalError("Create operation failed.");
        }
        RETURN_IF_STATUS_ERROR(DoAddOperation(op), "Add operation failed.");
    }
    return Status::OK();
}

Status OperationLogMemIndexer::DoAddOperation(OperationBase* operation)
{
    if (unlikely(!operation)) {
        AUTIL_LOG(DEBUG, "operation is null, do nothing");
        return Status::OK();
    }

    size_t opDumpSize = OperationDumper::GetDumpSize(operation);
    _operationMeta.Update(opDumpSize);

    assert(_blockInfo._curBlock);
    assert(_blockInfo._curBlock->Size() < _maxBlockSize);

    _blockInfo._curBlock->AddOperation(operation);
    if (NeedCreateNewBlock()) {
        AUTIL_LOG(DEBUG, "Need CreateNewBlock, current block count: %lu", _opBlocks->size());
        assert(!_opBlocks->empty());
        RETURN_IF_STATUS_ERROR(CreateNewBlock(_maxBlockSize), "create new block failed");
    }
    return Status::OK();
}

Status OperationLogMemIndexer::CreateNewBlock(size_t maxBlockSize)
{
    RETURN_IF_STATUS_ERROR(FlushBuildingOperationBlock(), "flush building operation block failed");
    std::shared_ptr<OperationBlock> newBlock(new OperationBlock(maxBlockSize));
    _opBlocks->push_back(newBlock);
    ResetBlockInfo(*_opBlocks);
    return Status::OK();
}

void OperationLogMemIndexer::ResetBlockInfo(const OperationBlockVec& opBlocks)
{
    assert(!opBlocks.empty());
    size_t noLastBlockOpCount = 0;
    size_t noLastBlockMemUse = 0;
    for (size_t i = 0; i < _opBlocks->size() - 1; ++i) {
        noLastBlockOpCount += (*_opBlocks)[i]->Size();
        noLastBlockMemUse += (*_opBlocks)[i]->GetTotalMemoryUse();
    }

    OperationBlockInfo blockInfo(noLastBlockOpCount, noLastBlockMemUse, *opBlocks.rbegin());
    _blockInfo = blockInfo;
}

std::shared_ptr<OperationFactory>
OperationLogMemIndexer::CreateOperationFactory(const std::shared_ptr<OperationLogConfig>& config)
{
    std::shared_ptr<OperationFactory> opFactory(new OperationFactory);
    opFactory->Init(config);
    return opFactory;
}

bool OperationLogMemIndexer::IsDirty() const { return _blockInfo.Size() > 0; }

Status OperationLogMemIndexer::Dump(autil::mem_pool::PoolBase* dumpPool, const file_system::DirectoryPtr& directory,
                                    const std::shared_ptr<indexlibv2::framework::DumpParams>& dumpParams)
{
    autil::ScopedLock lock(_dataLock);
    RETURN_IF_STATUS_ERROR(FlushBuildingOperationBlock(), "flush building operation block failed");
    OperationMeta dumpOpMeta;
    [[maybe_unused]] bool result = GetOperationMeta(dumpOpMeta);
    OperationDumper opDumper(dumpOpMeta);
    auto initStatus = opDumper.Init(*_opBlocks);
    if (!initStatus.IsOK()) {
        return initStatus;
    }
    auto dumpResult = opDumper.Dump(directory, dumpPool);
    if (!dumpResult.IsOK()) {
        return dumpResult;
    }

    return _operationFieldInfo->Store(directory->GetIDirectory());
}

std::pair<Status, std::shared_ptr<SegmentOperationIterator>>
OperationLogMemIndexer::CreateSegmentOperationIterator(size_t offset, const indexlibv2::framework::Locator& locator)
{
    autil::ScopedLock lock(_dataLock);
    if (offset >= _operationMeta.GetOperationCount()) {
        return {Status::OK(), nullptr};
    }
    assert(_opBlocks);
    std::shared_ptr<InMemSegmentOperationIterator> inMemSegOpIter(
        new InMemSegmentOperationIterator(_opConfig, _operationFieldInfo, _operationMeta, _segmentid, offset, locator));
    OperationBlockVec opBlocks;
    for (size_t i = 0; i < _opBlocks->size(); ++i) {
        opBlocks.push_back(std::shared_ptr<OperationBlock>((*_opBlocks)[i]->Clone()));
    }
    RETURN2_IF_STATUS_ERROR(inMemSegOpIter->Init(opBlocks), nullptr, "init operation iterator failed");
    return {Status::OK(), inMemSegOpIter};
}

Status OperationLogMemIndexer::FlushBuildingOperationBlock()
{
    if (_blockInfo._curBlock) {
        _operationMeta.EndOneBlock(_blockInfo._curBlock, (int64_t)_opBlocks->size() - 1);
    }
    return Status::OK();
}

bool OperationLogMemIndexer::GetOperationMeta(OperationMeta& operationMeta) const
{
    autil::ScopedLock lock(_dataLock);
    operationMeta = _operationMeta;
    size_t opBlockSize = _opBlocks->size();
    assert(opBlockSize > 0);
    operationMeta.EndOneBlock(_blockInfo._curBlock, opBlockSize - 1);
    return true;
}

std::string OperationLogMemIndexer::GetIndexName() const { return _opConfig->GetIndexName(); }
autil::StringView OperationLogMemIndexer::GetIndexType() const { return OPERATION_LOG_INDEX_TYPE_STR; }

void OperationLogMemIndexer::ValidateDocumentBatch(indexlibv2::document::IDocumentBatch* docBatch) {}
void OperationLogMemIndexer::FillStatistics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics) {}

void OperationLogMemIndexer::UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater)
{
    memUpdater->UpdateCurrentMemUse(GetCurrentMemoryUse());
    memUpdater->EstimateDumpTmpMemUse(_operationMeta.GetMaxOperationSerializeSize());
    memUpdater->EstimateDumpExpandMemUse(0);
    memUpdater->EstimateDumpedFileSize(_operationMeta.GetTotalDumpSize() + _operationMeta.GetLastBlockSerializeSize());
}
segmentid_t OperationLogMemIndexer::GetSegmentId() const { return _segmentid; }

bool OperationLogMemIndexer::IsSealed() const { return _isSealed; }

void OperationLogMemIndexer::Seal() { _isSealed = true; }

size_t OperationLogMemIndexer::GetCurrentMemoryUse() const { return _blockInfo.GetTotalMemoryUse(); }

size_t OperationLogMemIndexer::GetTotalOperationCount() const { return _operationMeta.GetOperationCount(); }

void OperationLogMemIndexer::RegisterMetrics(const std::shared_ptr<OperationLogMetrics>& metrics)
{
    _metrics = metrics;
}
std::pair<Status, std::shared_ptr<BatchOpLogIterator>> OperationLogMemIndexer::CreateBatchIterator()
{
    assert(false);
    return {Status::Unimplement(), nullptr};
}

} // namespace indexlib::index
