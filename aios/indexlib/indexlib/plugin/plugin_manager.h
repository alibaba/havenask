#ifndef __INDEXLIB_PLUGIN_PLUGINMANAGER_H
#define __INDEXLIB_PLUGIN_PLUGINMANAGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/module_info.h"
#include <autil/Lock.h>
#include "indexlib/misc/metric_provider.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, CounterMap);
IE_NAMESPACE_BEGIN(plugin);

class Module;

class PluginResource
{
public:
    PluginResource(const config::IndexPartitionSchemaPtr& _schema,
                   const config::IndexPartitionOptions& _options,
                   const util::CounterMapPtr& _counterMap,
                   const std::string& _pluginPath,
                   misc::MetricProviderPtr _metricProvider = nullptr)
        : schema(_schema)
        , options(_options)
        , counterMap(_counterMap)
        , pluginPath(_pluginPath)
        , metricProvider(_metricProvider)
    {}
    virtual ~PluginResource() {}
    config::IndexPartitionSchemaPtr schema;
    config::IndexPartitionOptions options;
    util::CounterMapPtr counterMap;
    std::string pluginPath;
    misc::MetricProviderPtr metricProvider;
};
DEFINE_SHARED_PTR(PluginResource);

class PluginManager
{
public:
    PluginManager(const std::string &moduleRootPath);
    ~PluginManager();

public:
    bool addModules(const config::ModuleInfos &moduleInfos);
    Module* getModule(const std::string &moduleName);
    void SetPluginResource(const PluginResourcePtr& resource)
    {
        mPluginResource = resource;
    }
    const PluginResourcePtr& GetPluginResource() const
    {
        return mPluginResource;
    }

    const std::string &GetModuleRootPath() const { return _moduleRootPath; }

public:
    static bool isBuildInModule(const std::string &moduleName) {
        return moduleName.empty() || moduleName == "BuildInModule";
    }
    static inline std::string GetPluginFileName(
        const std::string& pluginName)
    {
        return std::string("lib") + pluginName + std::string(".so");
    }
    
private:
    bool addModule(const config::ModuleInfo &moduleInfo);
    config::ModuleInfo getModuleInfo(const std::string &moduleName);
    void clear();
private:
    mutable autil::ThreadMutex mMapLock;     
    std::map<std::string, Module*> _name2moduleMap;
    std::map<std::string, config::ModuleInfo> _name2moduleInfoMap;
    std::string _moduleRootPath;
    PluginResourcePtr mPluginResource;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PluginManager);

IE_NAMESPACE_END(plugin);

#endif //__INDEXLIB_PLUGIN_PLUGINMANAGER_H
