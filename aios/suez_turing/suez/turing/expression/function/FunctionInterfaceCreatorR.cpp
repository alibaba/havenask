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
#include "suez/turing/expression/function/FunctionInterfaceCreatorR.h"

namespace suez {
namespace turing {

const std::string FunctionInterfaceCreatorR::RESOURCE_ID = "function_interface_creator_r";

const std::string FunctionInterfaceCreatorR::DYNAMIC_GROUP = "factory";

FunctionInterfaceCreatorR::FunctionInterfaceCreatorR() {}

FunctionInterfaceCreatorR::~FunctionInterfaceCreatorR() {}

void FunctionInterfaceCreatorR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ).dynamicResourceGroup(DYNAMIC_GROUP, BIND_DYNAMIC_RESOURCE_TO(_factorys));
}

bool FunctionInterfaceCreatorR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("config_path", _configPath, _configPath);
    ctx.Jsonize("modules", _funcConfig._modules, _funcConfig._modules);
    ctx.Jsonize("functions", _funcConfig._functionInfos, _funcConfig._functionInfos);
    return true;
}

navi::ErrorCode FunctionInterfaceCreatorR::init(navi::ResourceInitContext &ctx) {
    FunctionInterfaceCreatorPtr creator(new FunctionInterfaceCreator());
    FunctionCreatorResource funcCreatorResource;
    funcCreatorResource.cavaJitWrapper = _cavaJitWrapperR->getCavaJitWrapper().get();
    funcCreatorResource.resourceReader.reset(new suez::ResourceReader(_configPath));
    std::vector<SyntaxExpressionFactory *> addFactorys;
    for (auto factoryR : _factorys) {
        addFactorys.push_back(factoryR->getFactory());
    }
    if (!creator->init(_funcConfig, funcCreatorResource, addFactorys)) {
        NAVI_KERNEL_LOG(ERROR, "create FunctionInterfaceCreator failed, config path: [%s]", _configPath.c_str());
        return navi::EC_ABORT;
    }
    _creator = creator;
    return navi::EC_NONE;
}

const suez::turing::FunctionInterfaceCreatorPtr &FunctionInterfaceCreatorR::getCreator() const { return _creator; }

REGISTER_RESOURCE(FunctionInterfaceCreatorR);

} // namespace turing
} // namespace suez
