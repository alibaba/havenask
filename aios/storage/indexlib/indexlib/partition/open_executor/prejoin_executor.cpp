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
#include "indexlib/partition/open_executor/prejoin_executor.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::common;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PrejoinExecutor);

bool PrejoinExecutor::Execute(ExecutorResource& resource)
{
    util::ScopeLatencyReporter scopeTime(resource.mOnlinePartMetrics.GetprejoinLatencyMetric().get());
    bool ret = mJoinSegWriter->PreJoin();
    mJoinSegWriter.reset();
    return ret;
}
}} // namespace indexlib::partition
