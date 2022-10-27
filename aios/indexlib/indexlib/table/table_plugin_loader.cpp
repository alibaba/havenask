#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TablePluginLoader);

TablePluginLoader::TablePluginLoader() 
{
}

TablePluginLoader::~TablePluginLoader() 
{
}

PluginManagerPtr TablePluginLoader::Load(const string& pluginPath,
                                         const IndexPartitionSchemaPtr& schema,
                                         const IndexPartitionOptions& options)
{
    if (!schema)
    {
        IE_LOG(ERROR, "scheam is NULL");
        return PluginManagerPtr();
    }
    PluginManagerPtr pluginManager(
        new PluginManager(pluginPath));

    CustomizedTableConfigPtr customTableConfig = schema->GetCustomizedTableConfig();
    if (!customTableConfig)
    {
        IE_LOG(ERROR, "customized_table_config is NULL in Schema[%s]",
               schema->GetSchemaName().c_str());
        return PluginManagerPtr();
    }

    ModuleInfos moduleInfos;
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = customTableConfig->GetPluginName();
    moduleInfo.modulePath = PluginManager::GetPluginFileName(moduleInfo.moduleName);
    moduleInfos.push_back(moduleInfo);
    if (!pluginManager->addModules(moduleInfos))
    {
        return PluginManagerPtr();
    }
    return pluginManager;
}


IE_NAMESPACE_END(table);

