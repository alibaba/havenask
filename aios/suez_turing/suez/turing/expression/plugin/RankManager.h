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

#include <map>
#include <stddef.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "build_service/plugin/PlugInManager.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FunctionCreatorResource.h"

namespace suez {
namespace turing {

class RankManager;
class RankConfig;
class Scorer;

SUEZ_TYPEDEF_PTR(RankManager);

class RankManager {
public:
    RankManager(const std::string &configPath);
    ~RankManager();

public:
    static RankManagerPtr
    create(const std::string &configPath, const CavaPluginManager *cavaPluginManager, const RankConfig &rankConfig);

    void setPlugInManager(const build_service::plugin::PlugInManagerPtr &plugInManagerPtr) {
        _plugInManagerPtr = plugInManagerPtr;
    }
    bool addScorer(const std::string &name, Scorer *scorer) {
        auto it = _scorers.find(name);
        if (it != _scorers.end()) {
            return false;
        }
        _scorers[name] = scorer;
        return true;
    }
    Scorer *getScorer(const std::string &name) const {
        auto it = _scorers.find(name);
        if (it != _scorers.end()) {
            return it->second;
        }
        return NULL;
    }
    suez::ResourceReaderPtr getResourceReader() { return _resourceReaderPtr; }

private:
    suez::ResourceReaderPtr _resourceReaderPtr;
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    std::map<std::string, Scorer *> _scorers;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
