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
#include "indexlib/legacy/tools/partition_querier/executors/KvTableExecutorV2.h"

#include "indexlib/table/kv_table/KVTabletSessionReader.h"

namespace indexlibv2::tools {

Status KvTableExecutorV2::QueryKVTable(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                       const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                       const indexlibv2::base::PartitionQuery& query,
                                       PartitionResponse& partitionResponse)
{
    auto kvTabletReader = std::dynamic_pointer_cast<table::KVTabletSessionReader>(tabletReader);
    if (!kvTabletReader) {
        return Status::InvalidArgs("failed to cast tabletReader to KVTabletReader");
    }
    return kvTabletReader->QueryIndex(indexConfig, query, partitionResponse);
}

} // namespace indexlibv2::tools
