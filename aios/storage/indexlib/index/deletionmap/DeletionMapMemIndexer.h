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
#include "indexlib/util/ExpandableBitmap.h"

namespace indexlib::util {
class MMapAllocator;
class ExpandableBitmap;
} // namespace indexlib::util
namespace indexlib::file_system {
class FileWriter;
}
namespace indexlib::index {
class SingleDeletionMapBuilder;
}

namespace indexlibv2::index {
struct DeletionMapFileHeader;
class DeletionMapMetrics;

class DeletionMapMemIndexer : public IMemIndexer
{
public:
    DeletionMapMemIndexer(segmentid_t segId, bool isSortDump);
    ~DeletionMapMemIndexer();

    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(document::IDocumentBatch* docBatch) override;
    Status Build(const document::IIndexFields* indexFields, size_t n) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool,
                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                const std::shared_ptr<framework::DumpParams>& dumpParams) override;
    void Seal() override;
    void ValidateDocumentBatch(document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(document::IDocument* doc) override;
    bool IsValidField(const document::IIndexFields* fields) override;
    void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) override;
    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override;

    bool IsDeleted(docid_t docid) const;
    Status Delete(docid_t docid);
    uint32_t GetDeletedDocCount() const;
    void RegisterMetrics(const std::shared_ptr<DeletionMapMetrics>& metrics);
    bool IsDirty() const override;
    segmentid_t GetSegmentId() const;

private:
    void ProcessAddDocument(indexlibv2::document::IDocument* doc);
    friend class indexlib::index::SingleDeletionMapBuilder;

private:
    std::unique_ptr<indexlib::util::MMapAllocator> _allocator;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<DeletionMapMetrics> _metrics;
    std::unique_ptr<indexlib::util::ExpandableBitmap> _bitmap;
    std::unique_ptr<indexlib::util::ExpandableBitmap> _dumpingBitmap;
    size_t _docCount;
    segmentid_t _segmentId;
    bool _sortDump;

private:
    AUTIL_LOG_DECLARE();
    friend class DeletionMapMemIndexerTest;
};

inline bool DeletionMapMemIndexer::IsDeleted(docid_t docid) const
{
    if (docid >= _docCount) {
        return true;
    }
    uint32_t currentDocCount = _bitmap->GetValidItemCount();
    if (docid >= (docid_t)currentDocCount) {
        return false;
    }
    return _bitmap->Test(docid);
}

} // namespace indexlibv2::index
