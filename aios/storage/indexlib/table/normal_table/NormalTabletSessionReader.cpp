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
#include "indexlib/table/normal_table/NormalTabletSessionReader.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletSessionReader);

NormalTabletSessionReader::NormalTabletSessionReader(
    const std::shared_ptr<NormalTabletReader>& normalTabletReader,
    const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer)
    : CommonTabletSessionReader<NormalTabletReader>(normalTabletReader, memReclaimer)
{
}

NormalTabletSessionReader::~NormalTabletSessionReader() {}

const NormalTabletReader::AccessCounterMap& NormalTabletSessionReader::GetInvertedAccessCounter() const
{
    return _impl->GetInvertedAccessCounter();
}
const NormalTabletReader::AccessCounterMap& NormalTabletSessionReader::GetAttributeAccessCounter() const
{
    return _impl->GetAttributeAccessCounter();
}

Status NormalTabletSessionReader::QueryIndex(const base::PartitionQuery& query,
                                             base::PartitionResponse& partitionResponse) const
{
    return _impl->QueryIndex(query, partitionResponse);
}

Status NormalTabletSessionReader::QueryDocIds(const base::PartitionQuery& query,
                                              base::PartitionResponse& partitionResponse,
                                              std::vector<docid_t>& docids) const
{
    return _impl->QueryDocIds(query, partitionResponse, docids);
}

} // namespace indexlibv2::table
