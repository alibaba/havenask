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
#include "suez/turing/expression/function/FunctionFactoryBaseR.h"
#include "suez/turing/expression/function/SyntaxExpressionFactory.h"

namespace suez {
namespace turing {

bool FunctionFactoryBaseR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "config_path", _configPath, _configPath);
    return true;
}

navi::ErrorCode FunctionFactoryBaseR::init(navi::ResourceInitContext &ctx) {
    auto factory = createFactory();
    if (!factory) {
        NAVI_KERNEL_LOG(ERROR, "create factory failed");
        return navi::EC_ABORT;
    }
    auto resourceReader = std::make_shared<build_service::config::ResourceReader>(_configPath);
    build_service::KeyValueMap parameters;
    if (!factory->build_service::plugin::ModuleFactory::init(parameters, resourceReader)) {
        NAVI_KERNEL_LOG(ERROR, "init factory failed");
        return navi::EC_ABORT;
    }
    _factory = factory;
    return navi::EC_NONE;
}

}
}
