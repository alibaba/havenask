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
#include "indexlib/legacy/tools/partition_querier/executors/IndexTableExecutorV2.h"

#include "indexlib/table/normal_table/NormalTabletSessionReader.h"

namespace indexlibv2::tools {

Status IndexTableExecutorV2::QueryIndex(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                        const indexlibv2::base::PartitionQuery& query,
                                        indexlibv2::base::PartitionResponse& partitionResponse)
{
    auto normalTabletReader = std::dynamic_pointer_cast<table::NormalTabletSessionReader>(tabletReader);
    if (normalTabletReader == nullptr) {
        return Status::InvalidArgs("fail to cast tablet reader to normal tablet reader");
    }
    return normalTabletReader->QueryIndex(query, partitionResponse);
}

} // namespace indexlibv2::tools
