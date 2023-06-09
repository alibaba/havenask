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
#include "suez_navi/resource/FunctionInterfaceCreatorR.h"

using namespace suez::turing;

namespace suez_navi {

const std::string FunctionInterfaceCreatorR::RESOURCE_ID =
    "function_interface_creator_r";

FunctionInterfaceCreatorR::FunctionInterfaceCreatorR()
    : _cavaJitWrapperR(nullptr)
{
}

FunctionInterfaceCreatorR::~FunctionInterfaceCreatorR() {
}

void FunctionInterfaceCreatorR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID)
        .depend(CavaJitWrapperR::RESOURCE_ID, true,
                BIND_RESOURCE_TO(_cavaJitWrapperR));
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
    if (!creator->init(_funcConfig, funcCreatorResource)) {
        NAVI_KERNEL_LOG(
            ERROR, "create FunctionInterfaceCreator failed, config path: [%s]",
            _configPath.c_str());
        return navi::EC_ABORT;
    }
    _creator = creator;
    return navi::EC_NONE;
}

const suez::turing::FunctionInterfaceCreatorPtr &FunctionInterfaceCreatorR::getCreator() const {
    return _creator;
}

REGISTER_RESOURCE(FunctionInterfaceCreatorR);

}

