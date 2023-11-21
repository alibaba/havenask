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
#include "build_service/config/BuilderClusterConfig.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/config/IndexPartitionOptionsWrapper.h"

using namespace std;

namespace build_service { namespace config {

BS_LOG_SETUP(config, BuilderClusterConfig);

BuilderClusterConfig::BuilderClusterConfig() {}

BuilderClusterConfig::~BuilderClusterConfig() {}

bool BuilderClusterConfig::init(const string& clusterName_, const ResourceReader& resourceReader,
                                const string& mergeConfigName)
{
    clusterName = clusterName_;
    string clusterFileName = resourceReader.getClusterConfRelativePath(clusterName);
    if (!resourceReader.getConfigWithJsonPath(clusterFileName, "cluster_config.table_name", tableName) ||
        tableName.empty()) {
        string errorMsg = "get table_name from cluster file[" + clusterFileName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!resourceReader.getConfigWithJsonPath(clusterFileName, "cluster_config.overwrite_schema_name",
                                              overwriteSchemaName)) {
        string errorMsg = "get overwrite_schema_name from cluster file[" + clusterFileName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!resourceReader.getConfigWithJsonPath(clusterFileName, "cluster_config.enable_fast_slow_queue",
                                              enableFastSlowQueue)) {
        string errorMsg = "get enable_fast_slow_queue from cluster file[" + clusterFileName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!resourceReader.getConfigWithJsonPath(clusterFileName, "build_option_config", builderConfig)) {
        string errorMsg = "failed to parse build_option_config for: " + clusterName;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    return IndexPartitionOptionsWrapper::getIndexPartitionOptions(resourceReader, clusterName, mergeConfigName,
                                                                  indexOptions);
}

bool BuilderClusterConfig::validate() const
{
    if (!builderConfig.validate()) {
        string errorMsg = "validate build_option_config failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    try {
        indexOptions.Check();
    } catch (const autil::legacy::ExceptionBase& e) {
        string errorMsg = "check IndexPartitionOptions failed, error: " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    return true;
}

}} // namespace build_service::config
