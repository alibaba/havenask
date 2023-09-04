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
#include "sql/resource/Ha3FunctionModelConverter.h"

#include <map>
#include <string>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Utils.h"
#include "iquan/common/catalog/FunctionCommonDef.h"
#include "iquan/common/catalog/FunctionDef.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "sql/resource/Ha3FunctionDef.h"
#include "sql/resource/Ha3FunctionModel.h"

using namespace iquan;

namespace sql {

Status Ha3FunctionModelConverter::convert(FunctionModels &functionModels) {
    for (FunctionModel &model : functionModels.functions) {
        IQUAN_ENSURE_FUNC(Ha3FunctionModelConverter::convert(model));
    }
    return Status::OK();
}

Status Ha3FunctionModelConverter::convert(FunctionModel &functionModel) {
    auto reflectParamFn = [&](ParamTypeDef &paramDef) {
        if (paramDef.isMulti) {
            std::string newFieldType = paramDef.type;
            autil::StringUtil::toUpperCase(newFieldType);

            if (newFieldType != "ARRAY" && newFieldType != "MAP") {
                paramDef.valueType["type"] = paramDef.type;
                paramDef.type = "array";
            }
        } else {
            if (paramDef.type.empty()) {
                throw IquanException("paramDef.type is empty in " + functionModel.functionName);
            }
        }
    };

    // 2. convert params
    // 2.1 convert prototypes
    for (PrototypeDef &prototype : functionModel.functionContent.prototypes) {
        for (auto &returnType : prototype.returnTypes) {
            reflectParamFn(returnType);
        }

        for (ParamTypeDef &paramType : prototype.paramTypes) {
            reflectParamFn(paramType);
        }

        for (ParamTypeDef &accType : prototype.accTypes) {
            reflectParamFn(accType);
        }
    }
    return Status::OK();
}

Status Ha3FunctionModelConverter::convert(const Ha3FunctionModel &ha3FunctionModel,
                                          FunctionModels &functionModels) {
    Ha3FunctionsDef ha3FunctionsDef;
    IQUAN_ENSURE_FUNC(Utils::fromJson(ha3FunctionsDef, ha3FunctionModel.ha3FunctionsDefContent));

    for (auto &ha3FunctionDef : ha3FunctionsDef.functionDefList) {
        FunctionModel functionModel;

        // 1. common info
        functionModel.catalogName = ha3FunctionModel.catalogName;
        functionModel.databaseName = ha3FunctionModel.databaseName;
        functionModel.functionVersion = ha3FunctionModel.functionVersion;

        // 2. special info
        functionModel.functionName = ha3FunctionDef.functionName;
        functionModel.functionType = ha3FunctionDef.functionType;
        functionModel.isDeterministic = ha3FunctionDef.isDeterministic;
        functionModel.functionContentVersion = ha3FunctionDef.functionContentVersion;
        functionModel.functionContent = ha3FunctionDef.functionContent;
        functionModels.functions.emplace_back(functionModel);
    }
    return Status::OK();
}

} // namespace sql
