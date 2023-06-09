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
#include "indexlib/index/operation_log/SingleOperationLogBuilder.h"

#include "indexlib/index/operation_log/OperationLogMemIndexer.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SingleOperationLogBuilder);

SingleOperationLogBuilder::SingleOperationLogBuilder() {}

SingleOperationLogBuilder::~SingleOperationLogBuilder() {}

Status SingleOperationLogBuilder::Init(const indexlibv2::framework::TabletData& tabletData,
                                       const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    _operationLogConfig = std::dynamic_pointer_cast<OperationLogConfig>(indexConfig);
    if (_operationLogConfig == nullptr) {
        return Status::InternalError("Invalid operation log config.");
    }
    _buildingIndexer = nullptr;

    auto slice = tabletData.CreateSlice();
    for (auto it = slice.begin(); it != slice.end(); it++) {
        indexlibv2::framework::Segment* segment = it->get();
        if (segment->GetSegmentStatus() != indexlibv2::framework::Segment::SegmentStatus::ST_BUILDING) {
            continue;
        }
        auto [status, indexer] =
            segment->GetIndexer(_operationLogConfig->GetIndexType(), _operationLogConfig->GetIndexName());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "Get indexer failed. status: %s", status.ToString().c_str());
        }
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        assert(indexer != nullptr);
        _buildingIndexer = std::dynamic_pointer_cast<OperationLogMemIndexer>(indexer);
    }
    if (_buildingIndexer == nullptr) {
        return Status::InternalError("Summary builder init failed.");
    }

    return Status::OK();
}

Status SingleOperationLogBuilder::ProcessDocument(indexlibv2::document::IDocument* doc)
{
    return _buildingIndexer->ProcessDocument(doc);
}

} // namespace indexlib::index
