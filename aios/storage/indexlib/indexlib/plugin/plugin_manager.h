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
#ifndef __INDEXLIB_PLUGIN_PLUGINMANAGER_H
#define __INDEXLIB_PLUGIN_PLUGINMANAGER_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/module_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, CounterMap);
namespace indexlib { namespace plugin {

class Module;

class PluginResource
{
public:
    PluginResource(const config::IndexPartitionSchemaPtr& _schema, const config::IndexPartitionOptions& _options,
                   const util::CounterMapPtr& _counterMap, const std::string& _pluginPath,
                   util::MetricProviderPtr _metricProvider = util::MetricProviderPtr())
        : schema(_schema)
        , options(_options)
        , counterMap(_counterMap)
        , pluginPath(_pluginPath)
        , metricProvider(_metricProvider)
    {
    }
    virtual ~PluginResource() {}
    config::IndexPartitionSchemaPtr schema;
    config::IndexPartitionOptions options;
    util::CounterMapPtr counterMap;
    std::string pluginPath;
    util::MetricProviderPtr metricProvider;
};
DEFINE_SHARED_PTR(PluginResource);

class PluginManager
{
public:
    PluginManager(const std::string& moduleRootPath);
    ~PluginManager();

public:
    bool addModules(const config::ModuleInfos& moduleInfos);
    bool addModule(const config::ModuleInfo& moduleInfo);
    Module* getModule(const std::string& moduleName);
    void SetPluginResource(const PluginResourcePtr& resource) { mPluginResource = resource; }
    const PluginResourcePtr& GetPluginResource() const { return mPluginResource; }

    const std::string& GetModuleRootPath() const { return _moduleRootPath; }

public:
    static bool isBuildInModule(const std::string& moduleName)
    {
        return moduleName.empty() || moduleName == "BuildInModule";
    }
    static inline std::string GetPluginFileName(const std::string& pluginName)
    {
        return std::string("lib") + pluginName + std::string(".so");
    }

private:
    config::ModuleInfo getModuleInfo(const std::string& moduleName);
    void clear();
    std::pair<bool, Module*> loadModuleFromDir(const std::string& dir, config::ModuleInfo& moduleInfo);

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

}} // namespace indexlib::plugin

#endif //__INDEXLIB_PLUGIN_PLUGINMANAGER_H
