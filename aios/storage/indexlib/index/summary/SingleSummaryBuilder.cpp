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
#include "indexlib/index/summary/SingleSummaryBuilder.h"

#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/SummaryMemIndexer.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SingleSummaryBuilder);

SingleSummaryBuilder::SingleSummaryBuilder() {}

SingleSummaryBuilder::~SingleSummaryBuilder() {}

Status SingleSummaryBuilder::Init(const indexlibv2::framework::TabletData& tabletData,
                                  const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    _summaryIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SummaryIndexConfig>(indexConfig);
    if (_summaryIndexConfig == nullptr) {
        return Status::InternalError("Invalid summary index config.");
    }
    _buildingIndexer = nullptr;
    auto slice = tabletData.CreateSlice();
    for (auto it = slice.begin(); it != slice.end(); it++) {
        indexlibv2::framework::Segment* segment = it->get();
        if (segment->GetSegmentStatus() != indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING) {
            continue;
        }
        auto [status, indexer] =
            segment->GetIndexer(_summaryIndexConfig->GetIndexType(), _summaryIndexConfig->GetIndexName());
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        assert(indexer != nullptr);
        _buildingIndexer = std::dynamic_pointer_cast<indexlibv2::index::SummaryMemIndexer>(indexer).get();
    }
    if (_buildingIndexer == nullptr) {
        return Status::InternalError("Summary builder init failed.");
    }
    return Status::OK();
}

Status SingleSummaryBuilder::AddDocument(indexlibv2::document::IDocument* doc)
{
    assert(doc->GetDocOperateType() == ADD_DOC);
    return _buildingIndexer->AddDocument(doc);
}

const std::shared_ptr<indexlibv2::config::SummaryIndexConfig>& SingleSummaryBuilder::GetSummaryIndexConfig() const
{
    return _summaryIndexConfig;
}

} // namespace indexlib::index
