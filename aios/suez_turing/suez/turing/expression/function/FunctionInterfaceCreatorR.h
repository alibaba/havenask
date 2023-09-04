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

#include "build_service/plugin/ModuleInfo.h"
#include "navi/engine/Resource.h"
#include "suez/turing/expression/cava/common/CavaJitWrapperR.h"
#include "suez/turing/expression/function/FunctionFactoryBaseR.h"
#include "suez/turing/expression/function/FunctionInfo.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"

namespace suez {
namespace turing {

class FunctionInterfaceCreatorR : public navi::Resource {
public:
    FunctionInterfaceCreatorR();
    ~FunctionInterfaceCreatorR();
    FunctionInterfaceCreatorR(const FunctionInterfaceCreatorR &) = delete;
    FunctionInterfaceCreatorR &operator=(const FunctionInterfaceCreatorR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    const suez::turing::FunctionInterfaceCreatorPtr &getCreator() const;

public:
    static const std::string RESOURCE_ID;
    static const std::string DYNAMIC_GROUP;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(CavaJitWrapperR, _cavaJitWrapperR);
    std::set<FunctionFactoryBaseR *> _factorys;
    std::string _configPath;
    suez::turing::FuncConfig _funcConfig;
    suez::turing::FunctionInterfaceCreatorPtr _creator;
};
NAVI_TYPEDEF_PTR(FunctionInterfaceCreatorR);

} // namespace turing
} // namespace suez
