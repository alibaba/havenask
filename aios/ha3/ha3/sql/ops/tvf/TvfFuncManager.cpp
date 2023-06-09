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
#include "ha3/sql/ops/tvf/TvfFuncManager.h"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/plugin/PlugInManager.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/tvf/SqlTvfPluginConfig.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorFactory.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"
#include "ha3/sql/ops/tvf/builtin/BuiltinTvfFuncCreatorFactory.h"


namespace iquan {
class TvfModels;
} // namespace iquan

using namespace std;
using namespace build_service::plugin;
using namespace build_service::config;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, TvfFuncManager);

TvfFuncManager::TvfFuncManager(const ResourceReaderPtr &resourceReader)
    : PlugInManager(resourceReader, MODULE_SQL_TVF_FUNCTION)
{}

TvfFuncManager::~TvfFuncManager() {
    _builtinFactoryVec.clear();
}

bool TvfFuncManager::init(const SqlTvfPluginConfig &tvfPluginConfig, bool useBuiltin)
{
    if (useBuiltin) {
        _builtinFactoryVec.emplace_back(
                TvfFuncCreatorFactoryPtr(new BuiltinTvfFuncCreatorFactory()));
    }
    if (!initBuiltinFactory()) {

        return false;
    }
    if (!initPluginFactory(tvfPluginConfig)) {
        return false;
    }
    if (!addDefaultTvfInfo()) {
        SQL_LOG(ERROR, "add defalut tvf info failed.");
        return false;
    }
    if (!initTvfProfile(tvfPluginConfig.tvfProfileInfos)) {
        return false;
    }
    return true;
}

bool TvfFuncManager::initBuiltinFactory() {
    for (auto builtinFactory : _builtinFactoryVec) {
        if (!builtinFactory->registerTvfFunctions()) {
            SQL_LOG(ERROR, "register builtinTvfFuncCreatorFactory failed.");
            return false;
        }
        if (!addFunctions(builtinFactory.get())) {
            SQL_LOG(ERROR, "add builtin tvf functions failed.");
            return false;
        }
    }
    return true;
}

bool TvfFuncManager::addDefaultTvfInfo() {
    for (auto iter : _funcToCreator) {
        auto creator = iter.second;
        const auto &info = creator->getDefaultTvfInfo();
        if (info.empty()) {
            continue;
        }
        const std::string &tvfName = info.tvfName;
        if (_tvfNameToCreator.find(tvfName) != _tvfNameToCreator.end()) {
            SQL_LOG(ERROR, "add tvf function failed: conflict tvf name[%s].", tvfName.c_str());
            return false;
        }
        _tvfNameToCreator[tvfName] = creator;
    }
    return true;
}

bool TvfFuncManager::initPluginFactory(const SqlTvfPluginConfig &tvfPluginConfig) {
    if (!this->addModules(tvfPluginConfig.modules)) {
        SQL_LOG(ERROR, "load sql tvf function modules failed : %s.",
                ToJsonString(tvfPluginConfig.modules).c_str());
        return false;
    }
    auto &modules = tvfPluginConfig.modules;
    for (size_t i = 0; i < modules.size(); ++i) {
        const string &moduleName = modules[i].moduleName;
        Module *module = this->getModule(moduleName);
        if (module == nullptr) {
            SQL_LOG(ERROR, "Get module [%s] failed", moduleName.c_str());
            return false;
        }
        TvfFuncCreatorFactory *factory
            = dynamic_cast<TvfFuncCreatorFactory *>(module->getModuleFactory());
        if (factory == nullptr) {
            SQL_LOG(ERROR, "invalid TvfFuncCreatorFactory in module[%s].", moduleName.c_str());
            return false;
        }
        if (!factory->init(modules[i].parameters, _resourceReaderPtr, &_resourceContainer)) {
            SQL_LOG(ERROR, "init TvfFuncCreatorFactory in module[%s] failed.", moduleName.c_str());
            return false;
        }
        _pluginsFactory.push_back(make_pair(moduleName, factory));
        SQL_LOG(INFO, "add function module [%s]", moduleName.c_str());
        if (!factory->registerTvfFunctions()) {
            SQL_LOG(ERROR, "register tvf function failed, module[%s]", moduleName.c_str());
            return false;
        }
        if (!addFunctions(factory)) {
            SQL_LOG(ERROR, "add plugin tvf functions failed, module[%s]", moduleName.c_str());
            return false;
        }
    }
    return true;
}

bool TvfFuncManager::regTvfModels(iquan::TvfModels &tvfModels) {
    for (auto iter : _funcToCreator) {
        SQL_LOG(INFO, "try to register tvf models [%s]", iter.first.c_str());
        if (!iter.second->regTvfModels(tvfModels)) {
            SQL_LOG(ERROR, "register tvf models [%s] failed", iter.first.c_str());
            return false;
        }
    }
    return true;
}

bool TvfFuncManager::initTvfFuncCreator() {
    for (auto iter : _funcToCreator) {
        if (!iter.second->init(_resourceReaderPtr.get())) {
            SQL_LOG(ERROR, "init tvf function creator [%s] failed", iter.first.c_str());
            return false;
        }
    }
    return true;
}

bool TvfFuncManager::initTvfProfile(const SqlTvfProfileInfos &sqlTvfProfileInfos) {
    for (const auto &sqlTvfProfileInfo : sqlTvfProfileInfos) {
        auto iter = _funcToCreator.find(sqlTvfProfileInfo.funcName);
        if (iter == _funcToCreator.end()) {
            SQL_LOG(ERROR, "can not find tvf [%s]", sqlTvfProfileInfo.funcName.c_str());
            return false;
        }
        auto creator = iter->second;
        creator->addTvfFunction(sqlTvfProfileInfo);
        const string &tvfName = sqlTvfProfileInfo.tvfName;
        if (_tvfNameToCreator.find(tvfName) != _tvfNameToCreator.end()) {
            SQL_LOG(ERROR, "add tvf function failed: conflict tvf name[%s].",
                    tvfName.c_str());
            return false;
        }
        _tvfNameToCreator[tvfName] = creator;
    }
    if (!initTvfFuncCreator()) {
        return false;
    }
    return true;
}

bool TvfFuncManager::addFunctions(TvfFuncCreatorFactory *factory) {
    for (auto &iter : factory->getTvfFuncCreators()) {
        const string &funcName = iter.first;
        SQL_LOG(INFO, "try to add tvf function[%s].", funcName.c_str());
        if (_funcToCreator.find(funcName) != _funcToCreator.end()) {
            SQL_LOG(ERROR, "init tvf function failed: conflict function name[%s].",
                    funcName.c_str());
            return false;
        }
        _funcToCreator[funcName] = iter.second;
    }
    return true;
}

TvfFunc* TvfFuncManager::createTvfFunction(const std::string &tvfName) {
    auto it = _tvfNameToCreator.find(tvfName);
    if (it == _tvfNameToCreator.end()) {
        SQL_LOG(WARN, "tvf [%s]'s creator not found.", tvfName.c_str());
        return nullptr;
    }
    TvfFunc *tvfFunc = it->second->createFunction(tvfName);
    return tvfFunc;
}

} // namespace sql
} // namespace isearch
