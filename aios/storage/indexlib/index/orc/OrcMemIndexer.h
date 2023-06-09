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

#include <atomic>
#include <memory>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/file_system/ByteSliceWriter.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/orc/IOrcReader.h"
#include "indexlib/index/orc/MemoryOutputStream.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "orc/Type.hh"
#include "orc/Writer.hh"

namespace indexlib::util {
class BuildResourceMetrics;
class BuildResourceMetricsNode;
} // namespace indexlib::util

namespace orc {
class MemoryPool;
class WriterOptions;
} // namespace orc

namespace indexlibv2::document::extractor {
class IDocumentInfoExtractor;
}

namespace indexlibv2::index {

class OrcPoolAdapter;
class OrcMemBuffer;
class OrcMemBufferContainer;
class ColumnBuffer;

class OrcMemIndexer : public IMemIndexer
{
public:
    OrcMemIndexer();
    ~OrcMemIndexer();

    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(document::IDocumentBatch* docBatch) override;
    Status Build(const document::IIndexFields* indexFields, size_t n) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<indexlib::file_system::Directory>& directory,
                const std::shared_ptr<framework::DumpParams>& dumpParams) override;
    void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) override
    {
        // unimplemented
    }
    // not yet support real-time reading.
    std::unique_ptr<IOrcReader> CreateInMemoryReader(bool freezed) const { return nullptr; }
    void ValidateDocumentBatch(document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(document::IDocument* doc) override;
    bool IsValidField(const document::IIndexFields* fields) override;
    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::string GetIndexName() const override { return _config->GetIndexName(); }
    autil::StringView GetIndexType() const override { return ORC_INDEX_TYPE_STR; }
    void Seal() override {}

    // TODO: to optimize
    bool IsDirty() const override { return true; }

private:
    Status DocBatchToDocs(document::IDocumentBatch* docBatch,
                          std::vector<indexlib::document::AttributeDocument*>* docs) const;

    std::shared_ptr<config::OrcConfig> _config;
    autil::mem_pool::Pool _pool;
    indexlib::file_system::ByteSliceWriter _memWriter;
    MemoryOutputStream _outputStream;
    std::shared_ptr<orc::MemoryPool> _orcPool;
    std::unique_ptr<orc::Writer> _writer;
    std::shared_ptr<OrcMemBuffer> _memBuffer;
    std::unique_ptr<OrcMemBufferContainer> _memBufferContainer;

    std::atomic<uint64_t> _totalRecordCount;
    std::unique_ptr<document::extractor::IDocumentInfoExtractor> _docInfoExtractor;

private:
    friend class OrcMemIndexerTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
