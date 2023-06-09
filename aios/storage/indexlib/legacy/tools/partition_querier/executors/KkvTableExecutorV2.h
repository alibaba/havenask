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
#pragma once

#include "indexlib/base/Status.h"
#include "indexlib/base/proto/query.pb.h"

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::framework {
class ITabletReader;
}

namespace indexlibv2::tools {

class KkvTableExecutorV2
{
    using PartitionResponse = indexlibv2::base::PartitionResponse;

public:
    static Status QueryKKVTable(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                const indexlibv2::base::PartitionQuery& query, PartitionResponse& partitionResponse);
};

} // namespace indexlibv2::tools
