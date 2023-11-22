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

#include "build_service/common_define.h"
#include "build_service/config/RealtimeSchemaListKeeper.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class MultiClusterRealtimeSchemaListKeeper
{
public:
    MultiClusterRealtimeSchemaListKeeper();
    virtual ~MultiClusterRealtimeSchemaListKeeper();

    MultiClusterRealtimeSchemaListKeeper(const MultiClusterRealtimeSchemaListKeeper&) = delete;
    MultiClusterRealtimeSchemaListKeeper& operator=(const MultiClusterRealtimeSchemaListKeeper&) = delete;
    MultiClusterRealtimeSchemaListKeeper(MultiClusterRealtimeSchemaListKeeper&&) = delete;
    MultiClusterRealtimeSchemaListKeeper& operator=(MultiClusterRealtimeSchemaListKeeper&&) = delete;

public:
    // virtual for test
    virtual void init(const std::string& indexRoot, const std::vector<std::string>& clusterNames,
                      uint32_t generationId);
    bool updateConfig(const std::shared_ptr<config::ResourceReader>& resourceReader);
    std::shared_ptr<RealtimeSchemaListKeeper> getSingleClusterSchemaListKeeper(const std::string& clusterName);
    bool updateSchemaCache();

protected:
    std::vector<std::shared_ptr<RealtimeSchemaListKeeper>> _schemaListKeepers;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiClusterRealtimeSchemaListKeeper);

}} // namespace build_service::config
