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
#include "ha3/sql/ops/agg/AggFuncFactoryR.h"

#include <algorithm>
#include <utility>

#include "autil/mem_pool/Pool.h"
#include "ha3/sql/ops/agg/AggFunc.h"
#include "ha3/sql/ops/agg/AggFuncCreatorR.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/log/NaviLogger.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace isearch {
namespace sql {

const std::string AggFuncFactoryR::RESOURCE_ID = "sql_agg_func_factory";

AggFuncFactoryR::AggFuncFactoryR() {}

AggFuncFactoryR::~AggFuncFactoryR() {}

void AggFuncFactoryR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID)
        .dynamicDepend(BIND_DYNAMIC_RESOURCE_TO(_aggFuncCreators),
                       {"SQL_AGG_FUNC_AVG",
                        "SQL_AGG_FUNC_COUNT",
                        "SQL_AGG_FUNC_IDENTITY",
                        "SQL_AGG_FUNC_MAX",
                        "SQL_AGG_FUNC_MIN",
                        "SQL_AGG_FUNC_SUM",
                        "SQL_AGG_FUNC_ARBITRARY",
                        "SQL_AGG_FUNC_MAXLABEL",
                        "SQL_AGG_FUNC_GATHER",
                        "SQL_AGG_FUNC_MULTIGATHER",
                        "SQL_AGG_FUNC_LOGIC_SQL_AGG_FUNC_AND",
                        "SQL_AGG_FUNC_LOGIC_OR"});
}

bool AggFuncFactoryR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode AggFuncFactoryR::init(navi::ResourceInitContext &ctx) {
    for (const auto &creatorR : _aggFuncCreators) {
        const auto &funcName = creatorR->getAggFuncName();
        if (_funcToCreator.find(funcName) != _funcToCreator.end()) {
            NAVI_KERNEL_LOG(ERROR,
                            "init agg function plugins failed: conflict "
                            "function name[%s].",
                            funcName.c_str());
            return navi::EC_INIT_RESOURCE;
        }
        _funcToCreator[funcName] = creatorR;
        const auto &functionModel = creatorR->getFunctionModel();
        _functionModels.functions.emplace_back(functionModel);
    }
    return navi::EC_NONE;
}

const iquan::FunctionModels &AggFuncFactoryR::getFunctionModels() const {
    return _functionModels;
}

AggFunc *AggFuncFactoryR::createAggFunction(const std::string &aggFuncName,
                                            const std::vector<table::ValueType> &inputTypes,
                                            const std::vector<std::string> &inputFields,
                                            const std::vector<std::string> &outputFields,
                                            AggFuncMode mode) const {
    auto it = _funcToCreator.find(aggFuncName);
    if (it == _funcToCreator.end()) {
        NAVI_KERNEL_LOG(ERROR, "can not found agg func [%s]", aggFuncName.c_str());
        return nullptr;
    }
    AggFunc *aggFunc = it->second->createFunction(inputTypes, inputFields, outputFields, mode);
    if (aggFunc != nullptr) {
        aggFunc->setName(aggFuncName);
    }
    return aggFunc;
}

REGISTER_RESOURCE(AggFuncFactoryR);

} // namespace sql
} // namespace isearch
