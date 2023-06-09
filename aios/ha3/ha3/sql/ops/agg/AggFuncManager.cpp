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
#include "ha3/sql/ops/agg/AggFuncManager.h"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/agg/AggFunc.h"
#include "ha3/sql/ops/agg/AggFuncCreatorFactory.h"
#include "ha3/sql/ops/agg/AggFuncCreatorR.h"
#include "ha3/sql/ops/agg/SqlAggPluginConfig.h"
#include "ha3/sql/ops/agg/builtin/BuiltinAggFuncCreatorFactory.h"

using namespace std;
using namespace table;
using namespace build_service::plugin;
using namespace build_service::config;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AggFuncManager);

AggFuncManager::AggFuncManager(const ResourceReaderPtr &resourceReader)
    : PlugInManager(resourceReader, MODULE_SQL_AGG_FUNCTION)
{}

AggFuncManager::~AggFuncManager() {
}

bool AggFuncManager::init(const SqlAggPluginConfig &aggPluginConfig) {
    if (!initBuiltinFactory()) {
        SQL_LOG(ERROR, "init builtin factory failed");
        return false;
    }
    return initPluginFactory(aggPluginConfig);
}


bool AggFuncManager::initBuiltinFactory() {
    _builtinFactory.reset(new BuiltinAggFuncCreatorFactory());
    if (!registerFactoryFunctions(_builtinFactory.get())) {
        SQL_LOG(INFO, "register builtin factory [%p] functions failed", _builtinFactory.get());
        return false;
    }
    SQL_LOG(INFO, "add builtin function module [%p] success", _builtinFactory.get());
    return true;
}

bool AggFuncManager::initPluginFactory(const SqlAggPluginConfig &aggPluginConfig) {
    auto &modules = aggPluginConfig.modules;
    if (!this->addModules(modules)) {
        SQL_LOG(ERROR, "load sql agg function modules failed : %s.",
                ToJsonString(modules).c_str());
        return false;
    }
    for (size_t i = 0; i < modules.size(); ++i) {
        const string &moduleName = modules[i].moduleName;
        Module *module = this->getModule(moduleName);
        if (module == nullptr) {
            SQL_LOG(ERROR, "Get module [%s] failed", moduleName.c_str());
            return false;
        }
        AggFuncCreatorFactory *factory
            = dynamic_cast<AggFuncCreatorFactory *>(module->getModuleFactory());
        if (factory == nullptr) {
            SQL_LOG(ERROR, "invalid AggFuncCreatorFactory in module[%s].",
                    moduleName.c_str());
            return false;
        }
        if (!registerFactoryFunctions(factory)) {
            SQL_LOG(INFO, "register factory [%s] functions failed", moduleName.c_str());
            return false;
        }
        SQL_LOG(INFO, "add function module [%s] success", moduleName.c_str());
    }
    return true;
}

bool AggFuncManager::registerFactoryFunctions(AggFuncCreatorFactory *factory) {
    if (!factory->registerAggFunctions(&_interfaceManager)) {
        SQL_LOG(ERROR, "register agg function failed");
        return false;
    }
    if (!factory->registerFunctionInfos(_functionModels, _accSize)) {
        SQL_LOG(ERROR, "get agg function infos failed");
        return false;
    }
    return true;
}

AggFunc *AggFuncManager::createAggFunction(const std::string &aggFuncName,
                                           const std::vector<ValueType> &inputTypes,
                                           const std::vector<std::string> &inputFields,
                                           const std::vector<std::string> &outputFields,
                                           AggFuncMode mode) const
{
    auto creator = _interfaceManager.get<AggFuncCreatorR>(aggFuncName);
    if (creator == nullptr) {
        SQL_LOG(ERROR, "can not found agg func [%s]", aggFuncName.c_str());
        return nullptr;
    }
    AggFunc *aggFunc = creator->createFunction(inputTypes, inputFields, outputFields, mode);
    if (aggFunc != nullptr) {
        aggFunc->setName(aggFuncName);
    }
    return aggFunc;
}

} // namespace sql
} // namespace isearch
