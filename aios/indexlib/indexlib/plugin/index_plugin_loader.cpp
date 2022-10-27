#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(plugin);
IE_LOG_SETUP(plugin, IndexPluginLoader);

IndexPluginLoader::IndexPluginLoader() 
{
}

IndexPluginLoader::~IndexPluginLoader() 
{
}

string IndexPluginLoader::GetIndexModuleFileName(const string& indexerName)
{
    return PluginManager::GetPluginFileName(indexerName);
}

PluginManagerPtr IndexPluginLoader::Load(
        const string& moduleRootPath, const IndexSchemaPtr& indexSchema,
        const IndexPartitionOptions& options)
{
    ModuleInfos moduleInfos;    
    if (!FillOfflineModuleInfos(moduleRootPath, options, moduleInfos))
    {
        return PluginManagerPtr();
    }    
    if (!indexSchema)
    {
        IE_LOG(ERROR, "indexSchema is NULL");
        return PluginManagerPtr();
    }
    
    PluginManagerPtr pluginManager(
        new PluginManager(moduleRootPath));

    if (options.NeedLoadIndex()) {
        if (!FillCustomizedIndexModuleInfos(moduleRootPath, indexSchema, moduleInfos))
        {
            return PluginManagerPtr();
        }
    }

    if (!pluginManager->addModules(moduleInfos))
    {
        return PluginManagerPtr();
    }
    return pluginManager;
}

bool IndexPluginLoader::FillCustomizedIndexModuleInfos(
        const string& moduleRootPath, const IndexSchemaPtr& indexSchema,
        ModuleInfos& moduleInfos)
{
    set<string> indexerNames;
    auto indexConfigs = indexSchema->CreateIterator(false);
    for (auto it = indexConfigs->Begin(); it != indexConfigs->End(); it++)
    {
        const IndexConfigPtr& indexConfig = *it;
        if (indexConfig->GetIndexType() == it_customized)
        {
            auto typedIndexConfig =
                dynamic_pointer_cast<CustomizedIndexConfig>(indexConfig);
            indexerNames.insert(typedIndexConfig->GetIndexerName());
        }
    }
    for (const auto& indexerName : indexerNames)
    {
        const string& moduleFileName = GetIndexModuleFileName(indexerName);
        const string& moduleFilePath = PathUtil::JoinPath(
                moduleRootPath, moduleFileName);
        if (!FileSystemWrapper::IsExistIgnoreError(moduleFilePath))
        {
            IE_LOG(WARN, "module path [%s] does not exist", moduleFilePath.c_str());
        }
        ModuleInfo moduleInfo;
        moduleInfo.moduleName = indexerName;
        moduleInfo.modulePath = moduleFileName;;
        moduleInfos.push_back(moduleInfo);
    }
    return true;
}

bool IndexPluginLoader::FillOfflineModuleInfos(
    const string& moduleRootPath, const IndexPartitionOptions& options, ModuleInfos& moduleInfos)
{
    if (!options.IsOffline())
    {
        return true;
    }
    auto offlineModuleInfos = options.GetOfflineConfig().GetModuleInfos();
    for (auto& info : offlineModuleInfos)
    {
        string moduleFilePath = PathUtil::JoinPath(moduleRootPath, info.modulePath);
        if (!FileSystemWrapper::IsExistIgnoreError(moduleFilePath))
        {
            IE_LOG(ERROR, "module path [%s] is not existed.",
                   moduleFilePath.c_str());
            return false;
        }
        info.modulePath = moduleFilePath;
    }
    moduleInfos.insert(moduleInfos.end(), offlineModuleInfos.begin(), offlineModuleInfos.end());
    return true;
}

IE_NAMESPACE_END(plugin);

