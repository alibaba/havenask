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
#include "build_service_tasks/repartition/RepartitionDocFilterCreator.h"

#include "build_service/config/HashMode.h"
#include "build_service_tasks/repartition/RepartitionDocFilter.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace indexlib::index;
using namespace indexlib::merger;
using namespace indexlib::config;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, RepartitionDocFilterCreator);

RepartitionDocFilterCreator::RepartitionDocFilterCreator(const proto::Range& partitionRange,
                                                         const config::HashModeConfigPtr& hashModeConfig)
    : _partitionRange(partitionRange)
    , _hashModeConfig(hashModeConfig)
{
}

RepartitionDocFilterCreator::~RepartitionDocFilterCreator() {}

DocFilterPtr RepartitionDocFilterCreator::CreateDocFilter(const SegmentDirectoryPtr& segDir,
                                                          const IndexPartitionSchemaPtr& schema,
                                                          const IndexPartitionOptions& options)
{
    if (schema->GetRegionCount() != 1) {
        BS_LOG(ERROR, "not support multi region schema");
        return DocFilterPtr();
    }
    string regionName = schema->GetRegionSchema(0)->GetRegionName();
    HashMode hashMode;
    if (!_hashModeConfig->getHashMode(regionName, hashMode)) {
        BS_LOG(ERROR, "get HashMode failed for region [%s]", regionName.c_str());
        return DocFilterPtr();
    }
    auto hashFuncPtr = HashFuncFactory::createHashFunc(hashMode._hashFunction);
    string hashFieldName = hashMode._hashField;

    RepartitionDocFilterPtr repartitionDocFilter(new RepartitionDocFilter(schema, options, _partitionRange));
    if (repartitionDocFilter->Init(segDir, hashFieldName, hashFuncPtr)) {
        return repartitionDocFilter;
    }
    return DocFilterPtr();
}

}} // namespace build_service::task_base
