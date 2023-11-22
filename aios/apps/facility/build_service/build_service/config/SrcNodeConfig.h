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

#include <stdint.h>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_options.h"

namespace build_service { namespace config {

class SrcNodeConfig : public autil::legacy::Jsonizable
{
public:
    SrcNodeConfig();
    ~SrcNodeConfig();
    SrcNodeConfig(const SrcNodeConfig&) = default;
    SrcNodeConfig& operator=(const SrcNodeConfig&) = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;
    bool needSrcNode() const;

public:
    indexlib::config::IndexPartitionOptions options;
    std::string orderPreservingField;
    std::string blockCacheParam;
    uint64_t partitionMemoryInByte;
    uint64_t exceptionRetryTime;
    bool enableOrderPreserving;
    bool enableUpdateToAdd;
    std::string remoteRealtimeRootSuffix;

private:
    static const uint64_t RETRY_LIMIT = 4;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SrcNodeConfig);

}} // namespace build_service::config
