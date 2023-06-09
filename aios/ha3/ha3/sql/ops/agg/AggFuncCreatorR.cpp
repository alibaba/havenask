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
#include "ha3/sql/ops/agg/AggFuncCreatorR.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <memory>

#include "autil/StringUtil.h"
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/FunctionDef.h"
#include "navi/common.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/log/NaviLogger.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace isearch {
namespace sql {

AggFuncCreatorR::AggFuncCreatorR() {}

AggFuncCreatorR::~AggFuncCreatorR() {}

bool AggFuncCreatorR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode AggFuncCreatorR::init(navi::ResourceInitContext &ctx) {
    return navi::EC_NONE;
}

bool AggFuncCreatorR::initFunctionModel(const std::string &accTypes,
                                        const std::string &paramTypes,
                                        const std::string &returnTypes,
                                        const autil::legacy::json::JsonMap &properties) {
    _functionModel.functionName = getAggFuncName();
    _functionModel.functionType = "UDAF";
    _functionModel.isDeterministic = 1;
    _functionModel.functionContentVersion = "json_default_0.1";
    const auto &paramTypesVec = autil::StringUtil::split(paramTypes, "|");
    const auto &accTypesVec = autil::StringUtil::split(accTypes, "|");
    const auto &returnTypesVec = autil::StringUtil::split(returnTypes, "|");
    if ((paramTypesVec.size() != returnTypesVec.size())
        || (paramTypesVec.size() != accTypesVec.size())) {
        NAVI_KERNEL_LOG(ERROR,
                        "size of param types[%d] should be "
                        "equal to size of return types[%d] and size of acc types[%d]",
                        (int32_t)paramTypesVec.size(),
                        (int32_t)returnTypesVec.size(),
                        (int32_t)accTypesVec.size());
        return false;
    }
    for (size_t i = 0; i < paramTypesVec.size(); i++) {
        iquan::PrototypeDef singleDef;
        if (!parseIquanParamDef(paramTypesVec[i], singleDef.paramTypes)) {
            NAVI_KERNEL_LOG(ERROR, "parse param types failed: %s", paramTypesVec[i].c_str());
            return false;
        }
        if (!parseIquanParamDef(returnTypesVec[i], singleDef.returnTypes)) {
            NAVI_KERNEL_LOG(ERROR, "parse return types failed: %s", returnTypesVec[i].c_str());
            return false;
        }
        if (!parseIquanParamDef(accTypesVec[i], singleDef.accTypes)) {
            NAVI_KERNEL_LOG(ERROR, "parse return types failed: %s", returnTypesVec[i].c_str());
            return false;
        }
        _functionModel.functionContent.prototypes.emplace_back(singleDef);
    }
    _functionModel.functionContent.properties = properties;
    return true;
}

bool AggFuncCreatorR::parseIquanParamDef(const std::string &params,
                                         std::vector<iquan::ParamTypeDef> &paramsDef) {
    if (params.size() < 2 || params[0] != '[' || params[params.size() - 1] != ']') {
        NAVI_KERNEL_LOG(ERROR, "%s not array, expect [type, type, ...]", params.c_str());
        return false;
    }
    auto paramVec = autil::StringUtil::split(params.substr(1, params.size() - 2), ",");
    for (size_t i = 0; i < paramVec.size(); ++i) {
        autil::StringUtil::trim(paramVec[i]);
        if (paramVec[i].empty()) {
            NAVI_KERNEL_LOG(ERROR, "unexpected params[%lu] is empty", i);
            return false;
        }
        if (paramVec[i][0] == 'M') {
            paramsDef.emplace_back(iquan::ParamTypeDef(true, paramVec[i].substr(1)));
        } else {
            paramsDef.emplace_back(iquan::ParamTypeDef(false, paramVec[i]));
        }
    }
    return true;
}

const iquan::FunctionModel &AggFuncCreatorR::getFunctionModel() const {
    return _functionModel;
}

AggFunc *AggFuncCreatorR::createFunction(const std::vector<table::ValueType> &inputTypes,
                                         const std::vector<std::string> &inputFields,
                                         const std::vector<std::string> &outputFields,
                                         AggFuncMode mode) {
    assert(inputTypes.size() == inputFields.size());
    switch (mode) {
    case AggFuncMode::AGG_FUNC_MODE_LOCAL:
        return createLocalFunction(inputTypes, inputFields, outputFields);
        break;
    case AggFuncMode::AGG_FUNC_MODE_GLOBAL:
        return createGlobalFunction(inputTypes, inputFields, outputFields);
        break;
    case AggFuncMode::AGG_FUNC_MODE_NORMAL:
        return createNormalFunction(inputTypes, inputFields, outputFields);
        break;
    default:
        return nullptr;
    }
}

} // namespace sql
} // namespace isearch
