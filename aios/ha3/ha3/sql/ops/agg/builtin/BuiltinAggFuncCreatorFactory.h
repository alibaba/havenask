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
#include <memory>
#include <string>

#include "autil/Log.h"
#include "autil/plugin/InterfaceManager.h"
#include "ha3/sql/ops/agg/AggFuncCreatorFactory.h"

namespace isearch {
namespace sql {

class BuiltinAggFuncCreatorFactory : public AggFuncCreatorFactory {
public:
    BuiltinAggFuncCreatorFactory();
    ~BuiltinAggFuncCreatorFactory();

public:
    bool registerAggFunctions(autil::InterfaceManager *manager) override;
    virtual std::string getName() override {
        return "BuiltinAggFuncCreatorFactory";
    }

private:
    bool registerAggFunctionInfos() override;
    bool registerArbitraryFunctionInfos();
    bool registerMaxLabelFunctionInfos();
    bool registerGatherFunctionInfos();
    bool registerMultiGatherFunctionInfos();
    bool registerLogicAndFunctionInfos();
    bool registerLogicOrFunctionInfos();
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggFuncCreatorFactory> AggFuncCreatorFactoryPtr;
} // namespace sql
} // namespace isearch
