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
#include "build_service/config/SchemaUpdatableClusterConfig.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, SchemaUpdatableClusterConfig);

SchemaUpdatableClusterConfig::SchemaUpdatableClusterConfig() : partitionCount(1) {}

SchemaUpdatableClusterConfig::~SchemaUpdatableClusterConfig() {}

void SchemaUpdatableClusterConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("cluster_name", clusterName, clusterName);
    json.Jsonize("partition_count", partitionCount, partitionCount);
}

bool SchemaUpdatableClusterConfig::validate() const
{
    if (clusterName.empty()) {
        string errorMsg = "cluster name can't be empty";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (partitionCount == 0) {
        string errorMsg = "partition count can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::config
