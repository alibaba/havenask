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

using namespace iquan;

namespace sql {

Status Ha3FunctionModelConverter::convert(std::vector<iquan::FunctionModel> &functionModels) {
    for (FunctionModel &model : functionModels) {
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
                return Status(-1, "empty paramDef type: " + FastToJsonString(paramDef));
            }
        }
        return Status::OK();
    };

    // 2. convert params
    // 2.1 convert prototypes
    for (PrototypeDef &prototype : functionModel.functionDef.prototypes) {
        for (auto &returnType : prototype.returnTypes) {
            IQUAN_ENSURE_FUNC(reflectParamFn(returnType));
        }

        for (ParamTypeDef &paramType : prototype.paramTypes) {
            IQUAN_ENSURE_FUNC(reflectParamFn(paramType));
        }

        for (ParamTypeDef &accType : prototype.accTypes) {
            IQUAN_ENSURE_FUNC(reflectParamFn(accType));
        }
    }
    return Status::OK();
}

} // namespace sql
