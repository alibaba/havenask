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
#include "ha3/sql/ops/agg/AggFuncCreatorFactory.h"

#include <ext/alloc_traits.h>
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <memory>

#include "autil/StringUtil.h"
#include "ha3/sql/ops/agg/AggFuncCreatorR.h"
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/FunctionDef.h"
#include "iquan/common/catalog/FunctionModel.h"

using namespace std;
using namespace iquan;
namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, AggFuncCreatorFactory);

AggFuncCreatorFactory::AggFuncCreatorFactory() {}

AggFuncCreatorFactory::~AggFuncCreatorFactory() {
}

bool AggFuncCreatorFactory::registerAggFuncInfo(
        const std::string &name,
        const std::string &accTypes,
        const std::string &paramTypes,
        const std::string &returnTypes,
        const autil::legacy::json::JsonMap &properties) {
    FunctionModel def;
    def.functionName = name;
    def.functionType = "UDAF";
    def.isDeterministic = 1;
    def.functionContentVersion = "json_default_0.1";

    const vector<string> &paramTypesVec = autil::StringUtil::split(paramTypes, "|");
    const vector<string> &accTypesVec = autil::StringUtil::split(accTypes, "|");
    const vector<string> &returnTypesVec = autil::StringUtil::split(returnTypes, "|");
    if ((paramTypesVec.size() != returnTypesVec.size())
        || (paramTypesVec.size() != accTypesVec.size())) {
        SQL_LOG(ERROR,
                "size of param types[%d] should be "
                "equal to size of return types[%d] and size of acc types[%d]",
                (int32_t)paramTypesVec.size(),
                (int32_t)returnTypesVec.size(),
                (int32_t)accTypesVec.size());
        return false;
    }

    for (size_t i = 0; i < paramTypesVec.size(); i++) {
        PrototypeDef singleDef;
        if (!parseIquanParamDef(paramTypesVec[i], singleDef.paramTypes)) {
            SQL_LOG(ERROR, "parse param types failed: %s", paramTypesVec[i].c_str());
            return false;
        }
        if (!parseIquanParamDef(returnTypesVec[i], singleDef.returnTypes)) {
            SQL_LOG(ERROR, "parse return types failed: %s", returnTypesVec[i].c_str());
            return false;
        }
        if (!parseIquanParamDef(accTypesVec[i], singleDef.accTypes)) {
            SQL_LOG(ERROR, "parse return types failed: %s", returnTypesVec[i].c_str());
            return false;
        }

        if (!registerAccSize(name, singleDef.accTypes.size())) {
            SQL_LOG(ERROR, "register acc size failed: %s", accTypesVec[i].c_str());
            return false;
        }
        def.functionContent.prototypes.emplace_back(singleDef);
    }
    def.functionContent.properties = properties;
    _functionInfosDef.functions.emplace_back(def);

    return true;
}

bool AggFuncCreatorFactory::parseIquanParamDef(const std::string &params,
                                               std::vector<iquan::ParamTypeDef> &paramsDef) {
    if (params.size() < 2 || params[0] != '[' || params[params.size() - 1] != ']') {
        SQL_LOG(ERROR, "%s not array, expect [type, type, ...]", params.c_str());
        return false;
    }
    std::vector<std::string> paramVec
        = autil::StringUtil::split(params.substr(1, params.size() - 2), ",");
    for (size_t i = 0; i < paramVec.size(); ++i) {
        autil::StringUtil::trim(paramVec[i]);
        if (paramVec[i].empty()) {
            SQL_LOG(ERROR, "unexpected params[%lu] is empty", i);
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

bool AggFuncCreatorFactory::registerAccSize(const std::string &name, size_t i) {
    auto itr = _accSize.find(name);
    if (itr != _accSize.end()) {
        return itr->second == i;
    }
    _accSize[name] = i;
    return true;
}

bool AggFuncCreatorFactory::registerFunctionInfos(iquan::FunctionModels &functionModels,
                                                  std::map<std::string, size_t> &accSize) {
    if (!registerAggFunctionInfos()) {
        return false;
    }
    functionModels.merge(_functionInfosDef);
    for (auto it : _accSize) {
        if (accSize.find(it.first) != accSize.end()) {
            SQL_LOG(ERROR, "dup register agg func[%s]", it.first.c_str());
            return false;
        }
        accSize.insert(it);
    }
    return true;
}

void AggFuncCreatorFactory::destroy() {
    delete this;
}

} // namespace sql
} // namespace isearch
