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
#include "suez/turing/expression/plugin/SorterManager.h"

#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/plugin/DllWrapper.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"
#include "resource_reader/ResourceReader.h"
#include "suez/turing/expression/function/FunctionCreatorResource.h"
#include "suez/turing/expression/plugin/Sorter.h"
#include "suez/turing/expression/plugin/SorterConfig.h"
#include "suez/turing/expression/plugin/SorterModuleFactory.h"

using namespace std;
using namespace build_service::plugin;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SorterManager);

SorterManager::SorterManager(const std::string &configPath)
    : _resourceReaderPtr(new suez::ResourceReader(configPath)) {}

SorterManager::~SorterManager() {
    clear();
    _plugInManagerPtr.reset();
    _resourceReaderPtr.reset();
}

SorterManagerPtr SorterManager::create(const resource_reader::ResourceReaderPtr &resourceReader,
                                       const std::string &conf) {
    std::string content;
    if (!resourceReader->getConfigContent(conf, content)) {
        AUTIL_LOG(ERROR, "read %s failed", conf.c_str());
        return SorterManagerPtr();
    }
    SorterConfig sorterConfig;
    try {
        FastFromJsonString(sorterConfig, content);
    } catch (const std::exception &e) {
        AUTIL_LOG(WARN, "parse json string failed [%s], exception [%s]", content.c_str(), e.what());
        return SorterManagerPtr();
    }
    auto configPath = resourceReader->getConfigPath();
    return SorterManager::create(configPath, sorterConfig);
}

SorterManagerPtr SorterManager::create(const std::string &configPath, const SorterConfig &sorterConfig) {
    SorterManagerPtr sorterManagerPtr(new SorterManager(configPath));
    auto resourceReaderPtr = sorterManagerPtr->getResourceReader();
    AUTIL_LOG(TRACE3, "Init sorter modules, num [%ld]", sorterConfig.modules.size());
    PlugInManagerPtr plugInManagerPtr(new PlugInManager(resourceReaderPtr, MODULE_FUNC_SORTER));
    if (!plugInManagerPtr->addModules(sorterConfig.modules)) {
        AUTIL_LOG(ERROR, "Load sorter module failed : %s", FastToJsonString(sorterConfig.modules).c_str());
        return SorterManagerPtr();
    }
    sorterManagerPtr->setPlugInManager(plugInManagerPtr);
    AUTIL_LOG(TRACE3, "Init sorter infos, num [%ld]", sorterConfig.sorterInfos.size());
    for (auto &item : sorterConfig.sorterInfos) {
        Module *module = NULL;
        if (item.moduleName.empty()) {
            module = plugInManagerPtr->getModule();
        } else {
            module = plugInManagerPtr->getModule(item.moduleName);
        }
        if (nullptr == module) {
            AUTIL_LOG(ERROR, "Get module [%s] failed1. module[%p]", item.moduleName.c_str(), module);
            return SorterManagerPtr();
        }
        auto factory = (SorterModuleFactory *)module->getModuleFactory();
        auto className = item.className.empty() ? item.sorterName : item.className;
        auto sorter = factory->createSorter(className.c_str(), item.parameters, resourceReaderPtr.get());
        if (nullptr == sorter) {
            AUTIL_LOG(ERROR, "create sorter [%s] failed", item.sorterName.c_str());
            return SorterManagerPtr();
        }
        if (!sorterManagerPtr->addSorter(item.sorterName, sorter)) {
            delete sorter;
            sorter = nullptr;
            AUTIL_LOG(ERROR, "add sorter [%s] failed. module[%p]", item.sorterName.c_str(), module);
            return SorterManagerPtr();
        }
    }
    return sorterManagerPtr;
}

bool SorterManager::addSorter(const std::string &name, Sorter *sorter) {
    SorterMap::const_iterator it = _sorters.find(name);
    if (it != _sorters.end()) {
        AUTIL_LOG(ERROR, "sorter [%s] has already exist", name.c_str());
        return false;
    }
    _sorters[name] = sorter;
    return true;
}

void SorterManager::getSorterNames(std::set<std::string> &sorterNames) const {
    sorterNames.clear();
    for (SorterMap::const_iterator it = _sorters.begin(); it != _sorters.end(); ++it) {
        sorterNames.insert(it->first);
    }
}

Sorter *SorterManager::getSorter(const std::string &sorterName) const {
    SorterMap::const_iterator it = _sorters.find(sorterName);
    if (it == _sorters.end()) {
        return NULL;
    }
    return it->second;
}

void SorterManager::clear() {
    for (SorterMap::const_iterator it = _sorters.begin(); it != _sorters.end(); ++it) {
        delete it->second;
    }
    _sorters.clear();
}

} // namespace turing
} // namespace suez
