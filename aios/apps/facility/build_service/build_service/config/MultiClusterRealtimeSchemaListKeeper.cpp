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
#include "build_service/config/MultiClusterRealtimeSchemaListKeeper.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, MultiClusterRealtimeSchemaListKeeper);

MultiClusterRealtimeSchemaListKeeper::MultiClusterRealtimeSchemaListKeeper() {}

MultiClusterRealtimeSchemaListKeeper::~MultiClusterRealtimeSchemaListKeeper() {}

void MultiClusterRealtimeSchemaListKeeper::init(const std::string& indexRoot,
                                                const std::vector<std::string>& clusterNames, uint32_t generationId)
{
    for (const auto& clusterName : clusterNames) {
        auto keeper = std::make_shared<RealtimeSchemaListKeeper>();
        keeper->init(indexRoot, clusterName, generationId);
        _schemaListKeepers.push_back(keeper);
    }
}

bool MultiClusterRealtimeSchemaListKeeper::updateConfig(const std::shared_ptr<config::ResourceReader>& resourceReader)
{
    for (const auto& keeper : _schemaListKeepers) {
        auto clusterName = keeper->getClusterName();
        auto tabletSchema = resourceReader->getTabletSchema(clusterName);
        if (!keeper->addSchema(tabletSchema)) {
            return false;
        }
    }
    return true;
}

bool MultiClusterRealtimeSchemaListKeeper::updateSchemaCache()
{
    for (auto schemaListKeeper : _schemaListKeepers) {
        if (!schemaListKeeper->updateSchemaCache()) {
            BS_LOG(ERROR, "update [%s] schema cache failed", schemaListKeeper->getClusterName().c_str());
            return false;
        }
    }
    return true;
}

std::shared_ptr<RealtimeSchemaListKeeper>
MultiClusterRealtimeSchemaListKeeper::getSingleClusterSchemaListKeeper(const std::string& clusterName)
{
    for (auto schemaListKeeper : _schemaListKeepers) {
        if (schemaListKeeper->getClusterName() == clusterName) {
            return schemaListKeeper;
        }
    }
    return nullptr;
}

}} // namespace build_service::config
