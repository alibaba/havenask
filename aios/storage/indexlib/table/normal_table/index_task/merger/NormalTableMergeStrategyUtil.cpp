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
#include "indexlib/table/normal_table/index_task/merger/NormalTableMergeStrategyUtil.h"

#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableMergeStrategyUtil);

std::pair<Status, int64_t> NormalTableMergeStrategyUtil::GetDeleteDocCount(framework::Segment* segment)
{
    auto [status, indexer] =
        segment->GetIndexer(indexlibv2::index::DELETION_MAP_INDEX_TYPE_STR, indexlibv2::index::DELETION_MAP_INDEX_NAME);
    if (!status.IsOK()) {
        if (status.IsNotFound()) {
            return std::make_pair(Status::OK(), 0);
        }
        return std::make_pair(status, 0);
    }
    auto deletionmapDiskIndexer = std::dynamic_pointer_cast<indexlibv2::index::DeletionMapDiskIndexer>(indexer);
    assert(deletionmapDiskIndexer);
    return std::make_pair(Status::OK(), deletionmapDiskIndexer->GetDeletedDocCount());
}

}} // namespace indexlibv2::table
