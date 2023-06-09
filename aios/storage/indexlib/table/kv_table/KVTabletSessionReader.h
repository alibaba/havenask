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

#include "indexlib/table/common/CommonTabletSessionReader.h"
#include "indexlib/table/kv_table/KVTabletReader.h"

namespace indexlibv2::table {

class KVTabletSessionReader : public CommonTabletSessionReader<KVTabletReader>
{
public:
    KVTabletSessionReader(const std::shared_ptr<KVTabletReader>& kvTabletReader,
                          const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer);
    ~KVTabletSessionReader();

public:
    Status QueryIndex(const std::shared_ptr<config::IIndexConfig>& indexConfig, const base::PartitionQuery& query,
                      base::PartitionResponse& partitionResponse) const
    {
        return _impl->QueryIndex(indexConfig, query, partitionResponse);
    }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
