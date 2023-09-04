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
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "suez/turing/expression/cava/common/CavaJitWrapper.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"
#include "suez/turing/expression/common.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace suez {
namespace turing {
class FunctionInfo;

class CavaPluginManager {
public:
    CavaPluginManager(suez::turing::CavaJitWrapperPtr cavaJitWrapper,
                      kmonitor::MetricsReporter *reporter,
                      const std::vector<std::string> &autoRegFuncPkgs);
    ~CavaPluginManager();

private:
    CavaPluginManager(const CavaPluginManager &);
    CavaPluginManager &operator=(const CavaPluginManager &);

public:
    bool init(const std::vector<FunctionInfo> &functionInfos);
    static CavaModuleInfoPtr createScorerModule(::cava::CavaJitModulePtr &cavaJitModule, const std::string &scorerName);
    CavaModuleInfoPtr getCavaModuleInfo(const std::string &name) const {
        auto it = _moduleInfoMap.find(name);
        if (it == _moduleInfoMap.end()) {
            return CavaModuleInfoPtr();
        }
        return it->second;
    }
    suez::turing::CavaJitWrapper *getCavaJitWrapper() const { return _cavaJitWrapper.get(); }
    void reportFunctionPlugin() const;
    void reportFunctionException() const;

public:
    // for test
    size_t getModuleInfoMapSize() { return _moduleInfoMap.size(); }

private:
    void constructModuleInfoMap(const std::vector<FunctionInfo> &functionInfos);
    bool isLegalPkg(const std::string &pkgName);
    bool isAutoRegFuncPkg(const std::string &pkgName);

private:
    suez::turing::CavaJitWrapperPtr _cavaJitWrapper;
    // for function : processName -> CavaModuleInfo
    // for others : className -> CavaModuleInfo
    std::unordered_map<std::string, CavaModuleInfoPtr> _moduleInfoMap;
    kmonitor::MetricsReporter *_reporter;
    std::unordered_set<std::string> _autoRegFuncPkgs;
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(CavaPluginManager);
typedef std::map<std::string, CavaPluginManagerPtr> CavaPluginManagerMap;
SUEZ_TYPEDEF_PTR(CavaPluginManagerMap);
} // namespace turing
} // namespace suez
