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
#include "indexlib/plugin/index_plugin_loader.h"

#include <iosfwd>
#include <memory>
#include <set>
#include <vector>

#include "alog/Logger.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/offline_config.h"
#include "indexlib/config/schema_modify_operation.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;

using FSEC = indexlib::file_system::ErrorCode;
namespace indexlib { namespace plugin {
IE_LOG_SETUP(plugin, IndexPluginLoader);

IndexPluginLoader::IndexPluginLoader() {}

IndexPluginLoader::~IndexPluginLoader() {}

string IndexPluginLoader::GetIndexModuleFileName(const string& indexerName)
{
    return PluginManager::GetPluginFileName(indexerName);
}

PluginManagerPtr IndexPluginLoader::Load(const string& moduleRootPath, const IndexSchemaPtr& indexSchema,
                                         const IndexPartitionOptions& options)
{
    ModuleInfos moduleInfos;
    if (!FillOfflineModuleInfos(moduleRootPath, options, moduleInfos)) {
        return PluginManagerPtr();
    }
    if (!indexSchema) {
        IE_LOG(ERROR, "indexSchema is NULL");
        return PluginManagerPtr();
    }

    PluginManagerPtr pluginManager(new PluginManager(moduleRootPath));

    if (options.NeedLoadIndex()) {
        if (!FillCustomizedIndexModuleInfos(moduleRootPath, indexSchema, moduleInfos)) {
            return PluginManagerPtr();
        }
    }

    if (!pluginManager->addModules(moduleInfos)) {
        return PluginManagerPtr();
    }
    return pluginManager;
}

bool IndexPluginLoader::FillCustomizedIndexModuleInfos(const string& moduleRootPath, const IndexSchemaPtr& indexSchema,
                                                       ModuleInfos& moduleInfos)
{
    set<string> indexerNames;
    auto indexConfigs = indexSchema->CreateIterator(false);
    for (auto it = indexConfigs->Begin(); it != indexConfigs->End(); it++) {
        const IndexConfigPtr& indexConfig = *it;
        if (indexConfig->GetInvertedIndexType() == it_customized) {
            auto typedIndexConfig = dynamic_pointer_cast<CustomizedIndexConfig>(indexConfig);
            indexerNames.insert(typedIndexConfig->GetIndexerName());
        }
    }
    for (const auto& indexerName : indexerNames) {
        const string& moduleFileName = GetIndexModuleFileName(indexerName);
        const string& moduleFilePath = PathUtil::JoinPath(moduleRootPath, moduleFileName);
        bool isExist = false;
        auto ec = FslibWrapper::IsExist(moduleFilePath, isExist);
        THROW_IF_FS_ERROR(ec, "check module path [%s] exist failed", moduleFilePath.c_str());
        if (!isExist) {
            IE_LOG(WARN, "module path [%s] does not exist", moduleFilePath.c_str());
        }
        ModuleInfo moduleInfo;
        moduleInfo.moduleName = indexerName;
        moduleInfo.modulePath = moduleFileName;
        ;
        moduleInfos.push_back(moduleInfo);
    }
    return true;
}

bool IndexPluginLoader::FillOfflineModuleInfos(const string& moduleRootPath, const IndexPartitionOptions& options,
                                               ModuleInfos& moduleInfos)
{
    if (!options.IsOffline()) {
        return true;
    }
    auto offlineModuleInfos = options.GetOfflineConfig().GetModuleInfos();
    for (auto& info : offlineModuleInfos) {
        string moduleFilePath = PathUtil::JoinPath(moduleRootPath, info.modulePath);
        bool isExist = false;
        auto ec = FslibWrapper::IsExist(moduleFilePath, isExist);
        THROW_IF_FS_ERROR(ec, "check module [%s] isexist failed", moduleFilePath.c_str());
        if (!isExist) {
            IE_LOG(ERROR, "module path [%s] is not existed.", moduleFilePath.c_str());
            return false;
        }
        info.modulePath = moduleFilePath;
    }
    moduleInfos.insert(moduleInfos.end(), offlineModuleInfos.begin(), offlineModuleInfos.end());
    return true;
}

}} // namespace indexlib::plugin
