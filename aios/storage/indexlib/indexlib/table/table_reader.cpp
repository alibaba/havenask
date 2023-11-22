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
#include "indexlib/table/table_reader.h"

#include <iosfwd>

#include "indexlib/config/load_config_list.h"

using namespace std;

using namespace indexlib::config;
namespace indexlib { namespace table {
IE_LOG_SETUP(table, TableReader);

TableReader::TableReader() : /* mInterfaceId(DEFAULT_INTERFACE_ID), */ mForceSeekInfo(-1, -1), mExecutor(nullptr) {}

TableReader::~TableReader() { mExecutor = nullptr; }

bool TableReader::Init(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                       future_lite::Executor* executor, const util::MetricProviderPtr& metricProvider)
{
    mSchema = schema;
    mOptions = options;
    mExecutor = executor;
    mMetricProvider = metricProvider;
    // InitInterfaceId();
    return DoInit();
}

bool TableReader::DoInit() { return true; }

void TableReader::SetSegmentDependency(const std::vector<BuildingSegmentReaderPtr>& inMemSegments)
{
    mDependentInMemSegments = inMemSegments;
}
}} // namespace indexlib::table
