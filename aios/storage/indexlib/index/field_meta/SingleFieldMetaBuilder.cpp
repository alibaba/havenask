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
#include "indexlib/index/field_meta/SingleFieldMetaBuilder.h"

#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/FieldMetaMemIndexer.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SingleFieldMetaBuilder);

SingleFieldMetaBuilder::SingleFieldMetaBuilder() {}

SingleFieldMetaBuilder::~SingleFieldMetaBuilder() {}

Status SingleFieldMetaBuilder::Init(const indexlibv2::framework::TabletData& tabletData,
                                    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    _fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    if (_fieldMetaConfig == nullptr) {
        RETURN_STATUS_ERROR(InternalError, "Invalid field meta config.");
    }
    _buildingIndexer = nullptr;
    for (auto segment : tabletData.CreateSlice()) {
        if (segment->GetSegmentStatus() != indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING) {
            continue;
        }
        auto [status, indexer] =
            segment->GetIndexer(_fieldMetaConfig->GetIndexType(), _fieldMetaConfig->GetIndexName());
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        assert(indexer != nullptr);
        _buildingIndexer = std::dynamic_pointer_cast<FieldMetaMemIndexer>(indexer).get();
    }
    if (_buildingIndexer == nullptr) {
        RETURN_STATUS_ERROR(InternalError, "field meta builder init failed.");
    }
    return Status::OK();
}

Status SingleFieldMetaBuilder::Build(indexlibv2::document::IDocumentBatch* docBatch)
{
    return _buildingIndexer->Build(docBatch);
}

} // namespace indexlib::index
