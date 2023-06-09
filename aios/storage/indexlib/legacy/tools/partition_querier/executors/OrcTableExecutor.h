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
#include "indexlib/index/common/Term.h"

namespace indexlibv2::framework {
class ITabletReader;
}

namespace indexlib::config {
class FieldConfig;
}

namespace indexlibv2::tools {

class OrcTableExecutor
{
public:
    static Status QueryIndex(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                             const indexlibv2::base::PartitionQuery& query,
                             indexlibv2::base::PartitionResponse& partitionResponse);
    static Status QueryRowByDocId(const std::shared_ptr<framework::ITabletReader>& tabletReader, const docid_t docid,
                                  const std::vector<std::string>& attrs, indexlibv2::base::Row& row);
    static Status QueryIndexByDocId(const std::shared_ptr<framework::ITabletReader>& tabletReader,
                                    const std::vector<docid_t>& docids, const std::vector<std::string>& attrs,
                                    const int64_t limit, indexlibv2::base::PartitionResponse& partitionResponse);
};

} // namespace indexlibv2::tools
