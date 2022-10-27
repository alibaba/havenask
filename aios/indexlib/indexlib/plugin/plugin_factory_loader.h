#ifndef __INDEXLIB_PLUGIN_FACTORY_LOADER_H
#define __INDEXLIB_PLUGIN_FACTORY_LOADER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/plugin/plugin_manager.h"

IE_NAMESPACE_BEGIN(plugin);

class PluginFactoryLoader
{
public:
    PluginFactoryLoader();
    ~PluginFactoryLoader();
public:
    template <typename Factory, typename DefaultFactory>
    static Factory* GetFactory(const std::string& moduleName, const std::string& factorySuffix,
        const PluginManagerPtr& pluginManager);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PluginFactoryLoader);

/////////////////////////////////

template <typename Factory, typename DefaultFactory>
Factory* PluginFactoryLoader::GetFactory(const std::string& moduleName,
    const std::string& factorySuffix, const PluginManagerPtr& pluginManager)
{
    if (plugin::PluginManager::isBuildInModule(moduleName))
    {
        static DefaultFactory factory;
        return &factory;
    }
    auto modulePtr = pluginManager->getModule(moduleName);
    if (!modulePtr)
    {
        IE_LOG(ERROR, "get module[%s] failed", moduleName.c_str());
        return nullptr;
    }
    auto* factory = dynamic_cast<Factory*>(modulePtr->getModuleFactory(factorySuffix));
    if (!factory)
    {
        IE_LOG(ERROR, "get module factory[%s] failed", moduleName.c_str());
        return nullptr;
    }
    return factory;
}

IE_NAMESPACE_END(plugin);

#endif //__INDEXLIB_PLUGIN_FACTORY_LOADER_H
