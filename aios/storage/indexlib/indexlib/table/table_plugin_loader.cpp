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
#include "indexlib/table/table_plugin_loader.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::plugin;

namespace indexlib { namespace table {
IE_LOG_SETUP(table, TablePluginLoader);

TablePluginLoader::TablePluginLoader() {}

TablePluginLoader::~TablePluginLoader() {}

PluginManagerPtr TablePluginLoader::Load(const string& pluginPath, const IndexPartitionSchemaPtr& schema,
                                         const IndexPartitionOptions& options)
{
    if (!schema) {
        IE_LOG(ERROR, "scheam is NULL");
        return PluginManagerPtr();
    }
    PluginManagerPtr pluginManager(new PluginManager(pluginPath));

    CustomizedTableConfigPtr customTableConfig = schema->GetCustomizedTableConfig();
    if (!customTableConfig) {
        IE_LOG(ERROR, "customized_table_config is NULL in Schema[%s]", schema->GetSchemaName().c_str());
        return PluginManagerPtr();
    }

    ModuleInfos moduleInfos;
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = customTableConfig->GetPluginName();
    moduleInfo.modulePath = PluginManager::GetPluginFileName(moduleInfo.moduleName);
    moduleInfos.push_back(moduleInfo);
    if (!pluginManager->addModules(moduleInfos)) {
        return PluginManagerPtr();
    }
    return pluginManager;
}
}} // namespace indexlib::table
