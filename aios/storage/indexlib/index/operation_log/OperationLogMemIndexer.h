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
#pragma once

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/operation_log/OperationBlock.h"
#include "indexlib/index/operation_log/OperationBlockInfo.h"
#include "indexlib/index/operation_log/OperationFieldInfo.h"
#include "indexlib/index/operation_log/OperationLogIndexer.h"
#include "indexlib/index/operation_log/OperationMeta.h"

namespace indexlib::util {
class BuildResourceMetrics;
class BuildResourceMetricsNode;
} // namespace indexlib::util
namespace indexlibv2::config {
class IIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::document {
class IDocumentBatch;
}
namespace indexlibv2::index {
class OperationLogConfig;
} // namespace indexlibv2::index

namespace indexlib::index {
class OperationFactory;
class OperationLogMetrics;

class OperationLogMemIndexer : public indexlibv2::index::IMemIndexer, public OperationLogIndexer
{
public:
    OperationLogMemIndexer(segmentid_t segmentid);
    virtual ~OperationLogMemIndexer();

public:
    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& opConfig,
                indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(indexlibv2::document::IDocumentBatch* docBatch) override;
    Status Build(const indexlibv2::document::IIndexFields* indexFields, size_t n) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<file_system::Directory>& indexDirectory,
                const std::shared_ptr<indexlibv2::framework::DumpParams>&) override;

    segmentid_t GetSegmentId() const override;
    bool IsSealed() const override;
    bool GetOperationMeta(OperationMeta& operationMeta) const override;
    std::pair<Status, std::shared_ptr<SegmentOperationIterator>>
    CreateSegmentOperationIterator(size_t offset, const indexlibv2::framework::Locator& locator) override;
    std::pair<Status, std::shared_ptr<BatchOpLogIterator>> CreateBatchIterator() override;
    void ValidateDocumentBatch(indexlibv2::document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(indexlibv2::document::IDocument* doc) override { return true; }
    bool IsValidField(const indexlibv2::document::IIndexFields* fields) override { return true; }
    void FillStatistics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics) override;
    void UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override;
    size_t GetTotalOperationCount() const;
    void RegisterMetrics(const std::shared_ptr<OperationLogMetrics>& metrics);
    void Seal() override;
    bool IsDirty() const override;

protected:
    virtual size_t GetCurrentMemoryUse() const;
    virtual Status FlushBuildingOperationBlock();
    Status DoAddOperation(OperationBase* operation);
    void ResetBlockInfo(const OperationBlockVec& opBlocks);
    virtual Status CreateNewBlock(size_t maxBlockSize);

    // virtual for test
    virtual std::shared_ptr<OperationFactory>
    CreateOperationFactory(const std::shared_ptr<OperationLogConfig>& opConfig);
    virtual bool NeedCreateNewBlock() const { return _blockInfo._curBlock->Size() >= _maxBlockSize; }

private:
    Status ProcessDocument(indexlibv2::document::IDocument* doc);
    friend class SingleOperationLogBuilder;

protected:
    std::shared_ptr<OperationLogMetrics> _metrics;
    std::shared_ptr<OperationLogConfig> _opConfig;
    std::shared_ptr<OperationFieldInfo> _operationFieldInfo;
    size_t _maxBlockSize = 0;
    std::shared_ptr<OperationFactory> _operationFactory;
    std::shared_ptr<OperationBlockVec> _opBlocks;
    OperationBlockInfo _blockInfo;
    OperationMeta _operationMeta;
    mutable autil::RecursiveThreadMutex _dataLock;

private:
    segmentid_t _segmentid = INVALID_SEGMENTID;
    bool _isSealed = false;

private:
    friend class OperationLogMemIndexerTest;
    friend class OperationLogMemIndexerTest_TestEstimateDumpSize_Test;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
