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
#include "suez/turing/expression/cava/common/CavaPluginManager.h"

#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "cava/ast/ASTContext.h"
#include "cava/ast/ClassDecl.h"
#include "cava/ast/Name.h"
#include "cava/ast/Package.h"
#include "cava/codegen/CavaJitModule.h"
#include "cava/codegen/CavaModule.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h" // IWYU pragma: keep
#include "suez/turing/expression/cava/common/CavaASTUtil.h"
#include "suez/turing/expression/cava/common/CavaFunctionModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaScorerModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaSummaryModuleInfo.h"
#include "suez/turing/expression/function/FunctionInfo.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, CavaPluginManager);

CavaPluginManager::CavaPluginManager(suez::turing::CavaJitWrapperPtr cavaJitWrapper,
                                     kmonitor::MetricsReporter *reporter,
                                     const std::vector<std::string> &autoRegFuncPkgs)
    : _cavaJitWrapper(cavaJitWrapper)
    , _reporter(reporter)
    , _autoRegFuncPkgs(autoRegFuncPkgs.begin(), autoRegFuncPkgs.end()) {
    _autoRegFuncPkgs.insert("__self_register__");
}

CavaPluginManager::~CavaPluginManager() {}

bool CavaPluginManager::init(const std::vector<FunctionInfo> &functionInfos) {
    AUTIL_LOG(INFO, "function infos: [%s]", autil::legacy::FastToJsonString(functionInfos).c_str());
    constructModuleInfoMap(functionInfos);
    return true;
}

CavaModuleInfoPtr CavaPluginManager::createScorerModule(::cava::CavaJitModulePtr &cavaJitModule,
                                                        const std::string &scorerName) {
    if (!cavaJitModule) {
        return CavaModuleInfoPtr();
    }
    for (auto classDecl : cavaJitModule->_cavaModule->getASTContext().getClassDecls()) {
        const std::string &className = classDecl->getFullClassName();
        if (className == scorerName && CavaASTUtil::scorerMatcher(classDecl)) {
            return CavaScorerModuleInfo::create(className, cavaJitModule);
        }
    }
    return CavaModuleInfoPtr();
}

bool CavaPluginManager::isLegalPkg(const std::string &pkgName) { return ("ha3" != pkgName && "unsafe" != pkgName); }

bool CavaPluginManager::isAutoRegFuncPkg(const std::string &pkgName) {
    return _autoRegFuncPkgs.find(pkgName) != _autoRegFuncPkgs.end();
}

void CavaPluginManager::constructModuleInfoMap(const std::vector<FunctionInfo> &functionInfos) {
    if (!_cavaJitWrapper) {
        return;
    }
    auto ha3CavaModule = _cavaJitWrapper->getCavaModule();
    if (!ha3CavaModule) {
        return;
    }

    std::unordered_map<std::string, std::vector<FunctionInfo>> className2FuncInfo;
    for (auto &info : functionInfos) {
        auto it = className2FuncInfo.find(info.className);
        if (it == className2FuncInfo.end()) {
            std::vector<FunctionInfo> funcInfos = {info};
            className2FuncInfo[info.className] = funcInfos;
        } else {
            (it->second).push_back(info);
        }
    }

    for (auto classDecl : ha3CavaModule->getASTContext().getClassDecls()) {
        const std::string &className = classDecl->getFullClassName();
        ::cava::Package *pkg = classDecl->getPackage();
        std::string pkgName;
        if (nullptr != pkg) {
            ::cava::Name *name = pkg->getName();
            //用户package是ha3,那么诸如MatchDoc,如果是没有带package,manglename 不对的,会复用用户package name
            if (nullptr != name) {
                pkgName = name->getPostName();
                if (!isLegalPkg(pkgName)) {
                    AUTIL_LOG(
                        WARN, "the package of user plugins can not be \"ha3\" or \"unsafe\": %s", className.c_str());
                    continue;
                }
            }
        }

        AUTIL_LOG(DEBUG, "before create, cava class [%s]", className.c_str());
        autil::ScopedTime2 timer;
        CavaModuleInfoPtr cavaModuleInfo;
        auto cavaJitModule = _cavaJitWrapper->getCavaJitModule();
        if (CavaASTUtil::scorerMatcher(classDecl)) {
            autil::ScopedTime2 moduleTimer;
            cavaModuleInfo = CavaScorerModuleInfo::create(className, cavaJitModule);
            AUTIL_LOG(DEBUG,
                      "try create cava scorer module for class [%s] use [%lf] ms",
                      className.c_str(),
                      moduleTimer.done_ms());
        } else if (CavaASTUtil::summaryMatcher(classDecl)) {
            autil::ScopedTime2 moduleTimer;
            cavaModuleInfo = CavaSummaryModuleInfo::create(className, cavaJitModule);
            AUTIL_LOG(DEBUG,
                      "try create cava summary module for class [%s] use [%lf] ms",
                      className.c_str(),
                      moduleTimer.done_ms());
        }
        // TODO support more cava module type
        if (cavaModuleInfo) {
            AUTIL_LOG(INFO, "add cava class [%s] in moduleInfoMap use [%lf] ms", className.c_str(), timer.done_ms());
            _moduleInfoMap[className] = cavaModuleInfo;
        }
        if (CavaASTUtil::functionMatcher(classDecl)) {
            std::vector<FunctionInfo> funcInfos;
            {
                auto iter = className2FuncInfo.find(className);
                if (iter != className2FuncInfo.end()) {
                    // add function which contained in function config
                    funcInfos = iter->second;
                }
            }
            if (isAutoRegFuncPkg(pkgName)) {
                // add function in self register pkg
                FunctionInfo functionInfo;
                functionInfo.funcName = classDecl->getClassName();
                functionInfo.className = classDecl->getFullClassName();
                funcInfos.emplace_back(functionInfo);
            }
            if (!funcInfos.empty()) {
                CavaFunctionModuleInfo::create(cavaJitModule, classDecl, _moduleInfoMap, funcInfos);
            }
        }
    }
}

void CavaPluginManager::reportFunctionPlugin() const { REPORT_USER_MUTABLE_QPS(_reporter, "cava.functionPluginQps"); }

void CavaPluginManager::reportFunctionException() const {
    REPORT_USER_MUTABLE_QPS(_reporter, "cava.functionExceptionQps");
}

} // namespace turing
} // namespace suez
