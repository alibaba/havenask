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
#include "indexlib/index/ann/aitheta2/SingleAithetaBuilder.h"

#include "indexlib/index/ann/Common.h"
#include "indexlib/index/ann/aitheta2/AithetaMemIndexer.h"

namespace indexlib::index::ann {
AUTIL_LOG_SETUP(indexlib.index, SingleAithetaBuilder);

SingleAithetaBuilder::SingleAithetaBuilder() {}

SingleAithetaBuilder::~SingleAithetaBuilder() {}

Status SingleAithetaBuilder::Init(const indexlibv2::framework::TabletData& tabletData,
                                  const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    _buildingIndexer = nullptr;
    auto slice = tabletData.CreateSlice();
    for (auto it = slice.begin(); it != slice.end(); it++) {
        indexlibv2::framework::Segment* segment = it->get();
        if (segment->GetSegmentStatus() != indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING) {
            continue;
        }
        auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        assert(indexer != nullptr);
        _buildingIndexer = std::dynamic_pointer_cast<indexlibv2::index::ann::AithetaMemIndexer>(indexer).get();
    }
    if (_buildingIndexer == nullptr) {
        // Aitheta does not support updating built index yet, so if there is no building segment, we should skip
        // building.
        return Status::InternalError("No building indexer for aitheta.");
    }
    return Status::OK();
}

Status SingleAithetaBuilder::AddDocument(indexlibv2::document::IDocument* doc)
{
    assert(doc->GetDocOperateType() == ADD_DOC);
    return _buildingIndexer->BuildSingleDoc(doc);
}

bool SingleAithetaBuilder::ShouldSkipBuild() const { return _buildingIndexer->ShouldSkipBuild(); }

} // namespace indexlib::index::ann
