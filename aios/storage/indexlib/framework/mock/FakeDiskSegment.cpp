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
#include "indexlib/framework/mock/FakeDiskSegment.h"

#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"

#define SEGMENT_LOG(level, format, args...)                                                                            \
    AUTIL_LOG(level, "[%s] [%p] segment[%d] " format, _options->GetTabletName().c_str(), this, GetSegmentId(), ##args)

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, FakeDiskSegment);

FakeDiskSegment::FakeDiskSegment(const SegmentMeta& segmentMeta) : DiskSegment(segmentMeta) {}
FakeDiskSegment::~FakeDiskSegment() {}
Status FakeDiskSegment::Open(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController, OpenMode mode)
{
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    for (auto& [key, indexer] : _indexers) {
        auto [status, indexFactory] = indexFactoryCreator->Create(key.first);
        if (!status.IsOK()) {
            return status;
        }
        const auto& indexPath = indexFactory->GetIndexPath();
        auto [ec, indexDirectory] = GetSegmentDirectory()->GetIDirectory()->GetDirectory(indexPath);
        if (ec != indexlib::file_system::FSEC_OK) {
            return Status::InternalError("get index directory [%s] failed, ec[%d]",
                                         GetSegmentDirectory()->DebugString(indexPath).c_str(), ec);
        }
        const auto& indexConfig = GetSegmentSchema()->GetIndexConfig(key.first, key.second);
        if (!indexer) {
            indexlibv2::index::DiskIndexerParameter indexerParam;
            indexerParam.segmentId = GetSegmentId();
            indexerParam.segmentInfo = GetSegmentInfo();
            indexerParam.docCount = GetSegmentInfo()->GetDocCount();
            indexerParam.segmentMetrics = GetSegmentMetrics();
            indexer = indexFactory->CreateDiskIndexer(indexConfig, indexerParam);
        }
        if (auto status = indexer->Open(indexConfig, indexDirectory); !status.IsOK()) {
            return status;
        }
    }
    return Status::OK();
}

Status FakeDiskSegment::Reopen(const std::vector<std::shared_ptr<config::ITabletSchema>>& schemas)
{
    assert(false);
    return Status::OK();
}

std::pair<Status, std::shared_ptr<index::IIndexer>> FakeDiskSegment::GetIndexer(const std::string& type,
                                                                                const std::string& indexName)
{
    auto it = _indexers.find({type, indexName});
    if (it == _indexers.end()) {
        return {Status::NotFound(), nullptr};
    }
    return {Status::OK(), it->second};
}
void FakeDiskSegment::AddIndexer(const std::string& type, const std::string& indexName,
                                 std::shared_ptr<index::IIndexer> indexer)
{
    _indexers.emplace(std::make_pair(type, indexName), std::dynamic_pointer_cast<index::IDiskIndexer>(indexer));
}
void FakeDiskSegment::DeleteIndexer(const std::string& type, const std::string& indexName)
{
    _indexers.erase({type, indexName});
}

std::pair<Status, size_t> FakeDiskSegment::EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return {Status::OK(), 0};
}
size_t FakeDiskSegment::EvaluateCurrentMemUsed() { return 0; }
void FakeDiskSegment::SetSegmentId(segmentid_t segId) { _segmentMeta.segmentId = segId; }
void FakeDiskSegment::SetSegmentSchema(const std::shared_ptr<config::ITabletSchema>& schema)
{
    _segmentMeta.schema = schema;
}

} // namespace indexlibv2::framework
