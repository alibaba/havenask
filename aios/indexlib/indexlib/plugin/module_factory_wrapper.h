#ifndef __INDEXLIB_MODULE_FACTORY_WRAPPER_H
#define __INDEXLIB_MODULE_FACTORY_WRAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/config/module_info.h"

IE_NAMESPACE_BEGIN(plugin);

template <typename FactoryType>
class ModuleFactoryWrapper
{
public:
    #define DEFAULT_MODULE_FACTORY_NAME "Factory"
    ModuleFactoryWrapper(const std::string& factoryName =
                         DEFAULT_MODULE_FACTORY_NAME)
        : mFactoryName(factoryName)
        , mBuiltInFactory(nullptr)
        , mPluginFactory(nullptr)
    {}
    #undef DEFAULT_MODULE_FACTORY_NAME
    virtual ~ModuleFactoryWrapper()
    {
        DELETE_AND_SET_NULL(mBuiltInFactory);
        mPluginFactory = nullptr;
    }
    
public:
    bool Init(const PluginManagerPtr& pluginManager = PluginManagerPtr(),
              const std::string& moduleName = "");
    
    bool Init(const std::string &moduleRootPath,
              const config::ModuleInfo &moduleInfo);

    const PluginManagerPtr& GetPluginManager() const { return mPluginManager; }

    bool IsBuiltInFactory() const { return mBuiltInFactory != nullptr; }

protected:
    virtual FactoryType* CreateBuiltInFactory() const = 0;

protected:
    std::string mFactoryName;
    PluginManagerPtr mPluginManager;
    FactoryType *mBuiltInFactory;
    FactoryType *mPluginFactory;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(plugin, ModuleFactoryWrapper);

///////////////////////////////////////////////////////////////////////////////////

template <typename FactoryType>
inline bool ModuleFactoryWrapper<FactoryType>::Init(
        const PluginManagerPtr& pluginManager,
        const std::string& moduleName)
{
    mPluginManager = pluginManager;
    if (!mPluginManager || moduleName.empty())
    {
        IE_LOG(INFO, "init builtin module factory, moduleName[%s]!",
               moduleName.c_str());
        mBuiltInFactory = CreateBuiltInFactory();
        return mBuiltInFactory != nullptr;
    }
    
    Module* module = mPluginManager->getModule(moduleName);
    if (!module)
    {
        IE_LOG(ERROR, "not find module [%s]", moduleName.c_str());
        return false;
    }
    
    mPluginFactory = dynamic_cast<FactoryType*>(
            module->getModuleFactory(mFactoryName));
    if (!mPluginFactory)
    {
        IE_LOG(ERROR, "get %s from module [%s] fail!",
               mFactoryName.c_str(), moduleName.c_str());
        return false;
    }
    return true;
}

template <typename FactoryType>
inline bool ModuleFactoryWrapper<FactoryType>::Init(
        const std::string &moduleRootPath,
        const config::ModuleInfo &moduleInfo)
{
    PluginManagerPtr pluginMgr(new PluginManager(moduleRootPath));
    config::ModuleInfos moduleInfos;
    moduleInfos.push_back(moduleInfo);
    if (!pluginMgr->addModules(moduleInfos))
    {
        IE_LOG(ERROR, "add module [%s] fail!", moduleInfo.moduleName.c_str());
        return false;
    }
    return Init(pluginMgr, moduleInfo.moduleName);
}

IE_NAMESPACE_END(plugin);

#endif //__INDEXLIB_MODULE_FACTORY_WRAPPER_H
