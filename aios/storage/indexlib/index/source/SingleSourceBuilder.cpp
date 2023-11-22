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
#include "indexlib/index/source/SingleSourceBuilder.h"

#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/SourceMemIndexer.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SingleSourceBuilder);

SingleSourceBuilder::SingleSourceBuilder() {}

SingleSourceBuilder::~SingleSourceBuilder() {}

Status SingleSourceBuilder::Init(const indexlibv2::framework::TabletData& tabletData,
                                 const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    _sourceIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SourceIndexConfig>(indexConfig);
    if (_sourceIndexConfig == nullptr) {
        RETURN_STATUS_ERROR(InternalError, "Invalid source index config.");
    }
    _buildingIndexer = nullptr;
    for (auto segment : tabletData.CreateSlice()) {
        if (segment->GetSegmentStatus() != indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING) {
            continue;
        }
        auto [status, indexer] =
            segment->GetIndexer(_sourceIndexConfig->GetIndexType(), _sourceIndexConfig->GetIndexName());
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        assert(indexer != nullptr);
        _buildingIndexer = std::dynamic_pointer_cast<indexlibv2::index::SourceMemIndexer>(indexer).get();
    }
    if (_buildingIndexer == nullptr) {
        RETURN_STATUS_ERROR(InternalError, "Source builder init failed.");
    }
    return Status::OK();
}

Status SingleSourceBuilder::AddDocument(indexlibv2::document::IDocument* doc)
{
    assert(doc->GetDocOperateType() == ADD_DOC);
    return _buildingIndexer->AddDocument(doc);
}

} // namespace indexlib::index
