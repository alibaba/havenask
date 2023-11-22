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
#pragma once

#include <memory>
#include <string>

#include "alog/Logger.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/plugin_manager.h"

namespace indexlib { namespace plugin {

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
Factory* PluginFactoryLoader::GetFactory(const std::string& moduleName, const std::string& factorySuffix,
                                         const PluginManagerPtr& pluginManager)
{
    if (plugin::PluginManager::isBuildInModule(moduleName)) {
        static DefaultFactory factory;
        return &factory;
    }
    auto modulePtr = pluginManager->getModule(moduleName);
    if (!modulePtr) {
        IE_LOG(ERROR, "get module[%s] failed", moduleName.c_str());
        return nullptr;
    }
    auto* factory = dynamic_cast<Factory*>(modulePtr->getModuleFactory(factorySuffix));
    if (!factory) {
        IE_LOG(ERROR, "get module factory[%s] failed", moduleName.c_str());
        return nullptr;
    }
    return factory;
}

}} // namespace indexlib::plugin
