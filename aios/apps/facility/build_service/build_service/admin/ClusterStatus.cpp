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
#include "build_service/admin/ClusterStatus.h"

#include <iosfwd>

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ClusterStatus);

ClusterStatus::ClusterStatus() : roleType(proto::ROLE_UNKNOWN), stopTime(-1), schemaVersion(0), processorCheckpoint(0)
{
}

ClusterStatus::~ClusterStatus() {}

bool ClusterStatus::operator!=(const ClusterStatus& other) const { return !(*this == other); }

bool ClusterStatus::operator==(const ClusterStatus& other) const
{
    return roleType == other.roleType && stopTime == other.stopTime && configPath == other.configPath &&
           mergeConfigName == other.mergeConfigName && schemaVersion == other.schemaVersion &&
           processorCheckpoint == other.processorCheckpoint;
}

void ClusterStatus::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("role_type", roleType, roleType);
    json.Jsonize("stop_time", stopTime, stopTime);
    json.Jsonize("config_path", configPath, configPath);
    json.Jsonize("merge_config_name", mergeConfigName, mergeConfigName);
    json.Jsonize("schema_version", schemaVersion, schemaVersion);
    json.Jsonize("processor_checkpoint", processorCheckpoint, processorCheckpoint);
}

}} // namespace build_service::admin
