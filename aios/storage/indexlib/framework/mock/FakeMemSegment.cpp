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
#include "indexlib/framework/mock/FakeMemSegment.h"

#include "indexlib/framework/DumpParams.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::framework {
#define SEGMENT_LOG(level, format, args...)                                                                            \
    AUTIL_LOG(level, "[%s] [%p] segment[%d] " format, _options->GetTabletName().c_str(), this, GetSegmentId(), ##args)

FakeMemSegment::FakeMemSegment(const SegmentMeta& segmentMeta) : MemSegment(segmentMeta) {}
FakeMemSegment::~FakeMemSegment() {}

Status FakeMemSegment::Open(const BuildResource& resource, indexlib::framework::SegmentMetrics* lastSegmentMetrics)
{
    _buildResourceMetrics = resource.buildResourceMetrics;
    return Status::OK();
}
Status FakeMemSegment::Build(document::IDocumentBatch* batch)
{
    assert(batch != nullptr);

    for (auto& [_, indexer] : _indexers) {
        if (auto status = indexer->Build(batch); !status.IsOK()) {
            return status;
        }
    }
    _segmentMeta.segmentInfo->docCount += batch->GetAddedDocCount();
    _isDirty = true;
    return Status::OK();
}
std::shared_ptr<indexlib::util::BuildResourceMetrics> FakeMemSegment::GetBuildResourceMetrics() const
{
    return _buildResourceMetrics;
}
bool FakeMemSegment::NeedDump() const { return false; }
std::pair<Status, std::vector<std::shared_ptr<SegmentDumpItem>>> FakeMemSegment::CreateSegmentDumpItems()
{
    return std::make_pair<Status, std::vector<std::shared_ptr<SegmentDumpItem>>>(Status::Unimplement(), {});
}
void FakeMemSegment::EndDump() { assert(false); }
void FakeMemSegment::Seal() { assert(false); }
bool FakeMemSegment::IsDirty() const { return _isDirty; }

std::pair<Status, std::shared_ptr<index::IIndexer>> FakeMemSegment::GetIndexer(const std::string& type,
                                                                               const std::string& indexName)
{
    auto it = _indexers.find({type, indexName});
    if (it == _indexers.end()) {
        return {Status::NotFound(), nullptr};
    }
    return {Status::OK(), it->second};
}

void FakeMemSegment::AddIndexer(const std::string& type, const std::string& indexName,
                                std::shared_ptr<index::IIndexer> indexer)
{
    _indexers.emplace(std::make_pair(type, indexName), std::dynamic_pointer_cast<index::IMemIndexer>(indexer));
}

Status FakeMemSegment::Dump()
{
    auto dumpPool = std::make_unique<indexlib::util::SimplePool>();
    auto dumpParams = std::make_shared<indexlibv2::framework::DumpParams>();
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    for (auto& [key, indexer] : _indexers) {
        auto [status, indexFactory] = indexFactoryCreator->Create(key.first);
        if (!status.IsOK()) {
            return status;
        }
        auto indexDirectory = GetSegmentDirectory()->MakeDirectory(indexFactory->GetIndexPath());
        if (auto status = indexer->Dump(dumpPool.get(), indexDirectory, dumpParams); !status.IsOK()) {
            return status;
        }
    }
    return Status::OK();
}

void FakeMemSegment::DeleteIndexer(const std::string& type, const std::string& indexName)
{
    _indexers.erase({type, indexName});
}

size_t FakeMemSegment::EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) { return 0; }
size_t FakeMemSegment::EvaluateCurrentMemUsed() { return 0; }
void FakeMemSegment::SetSegmentId(segmentid_t segId) { _segmentMeta.segmentId = segId; }
void FakeMemSegment::SetSegmentSchema(const std::shared_ptr<config::ITabletSchema>& schema)
{
    _segmentMeta.schema = schema;
}

} // namespace indexlibv2::framework
