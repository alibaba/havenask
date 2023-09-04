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
#include "suez/turing/expression/plugin/RankManager.h"

#include <cstddef>
#include <map>
#include <memory>
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
#include "suez/turing/expression/function/FunctionCreatorResource.h"
#include "suez/turing/expression/plugin/RankConfig.h"
#include "suez/turing/expression/plugin/Scorer.h"
#include "suez/turing/expression/plugin/ScorerModuleFactory.h"

namespace suez {
namespace turing {
class CavaPluginManager;
} // namespace turing
} // namespace suez

using namespace std;
using namespace build_service::plugin;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, RankManager);

RankManager::RankManager(const std::string &configPath) : _resourceReaderPtr(new suez::ResourceReader(configPath)) {}

RankManager::~RankManager() {
    for (auto it = _scorers.begin(); it != _scorers.end(); it++) {
        delete it->second;
    }
    _scorers.clear();
    _plugInManagerPtr.reset();
    _resourceReaderPtr.reset();
}

RankManagerPtr RankManager::create(const std::string &configPath,
                                   const CavaPluginManager *cavaPluginManager,
                                   const RankConfig &rankConfig) {
    RankManagerPtr rankManagerPtr(new RankManager(configPath));
    auto resourceReaderPtr = rankManagerPtr->getResourceReader();
    AUTIL_LOG(TRACE3, "Init rank modules, num [%ld]", rankConfig.modules.size());
    PlugInManagerPtr plugInManagerPtr(new PlugInManager(resourceReaderPtr, MODULE_FUNC_SCORER));
    if (!plugInManagerPtr->addModules(rankConfig.modules)) {
        AUTIL_LOG(ERROR, "Load scorer module failed : %s", FastToJsonString(rankConfig.modules).c_str());
        return RankManagerPtr();
    }
    rankManagerPtr->setPlugInManager(plugInManagerPtr);
    AUTIL_LOG(TRACE3, "Init rank infos, num [%ld]", rankConfig.rankInfos.size());
    for (auto &item : rankConfig.rankInfos) {
        Module *module = NULL;
        if (item.moduleName.empty()) {
            module = plugInManagerPtr->getModule();
        } else {
            module = plugInManagerPtr->getModule(item.moduleName);
        }
        if (!module) {
            AUTIL_LOG(WARN, "Get module [%s] failed1. module[%p]", item.moduleName.c_str(), module);
            return RankManagerPtr();
        }
        auto factory = (ScorerModuleFactory *)module->getModuleFactory();
        auto scorer =
            factory->createScorer(item.className.c_str(), item.parameters, resourceReaderPtr.get(), cavaPluginManager);
        if (!rankManagerPtr->addScorer(item.scorerName, scorer)) {
            AUTIL_LOG(WARN, "add scorer [%s] failed1. module[%p]", item.scorerName.c_str(), module);
            return RankManagerPtr();
        }
    }
    return rankManagerPtr;
}

} // namespace turing
} // namespace suez
