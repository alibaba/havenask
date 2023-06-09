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

#include <string>
#include <vector>

#include "ha3/sql/ops/agg/AggFuncMode.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/log/NaviLogger.h"
#include "table/DataCommon.h"

namespace iquan {
class ParamTypeDef;
} // namespace iquan
namespace navi {
class ResourceInitContext;
} // namespace navi

namespace isearch {
namespace sql {

class AggFunc;

class AggFuncCreatorR : public navi::Resource {
public:
    AggFuncCreatorR();
    ~AggFuncCreatorR();
    AggFuncCreatorR(const AggFuncCreatorR &) = delete;
    AggFuncCreatorR &operator=(const AggFuncCreatorR &) = delete;

public:
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    const iquan::FunctionModel &getFunctionModel() const;
    AggFunc *createFunction(const std::vector<table::ValueType> &inputTypes,
                            const std::vector<std::string> &inputFields,
                            const std::vector<std::string> &outputFields,
                            AggFuncMode mode);
    virtual std::string getAggFuncName() const = 0;

private:
    virtual AggFunc *createLocalFunction(const std::vector<table::ValueType> &inputTypes,
                                         const std::vector<std::string> &inputFields,
                                         const std::vector<std::string> &outputFields)
        = 0;
    virtual AggFunc *createGlobalFunction(const std::vector<table::ValueType> &inputTypes,
                                          const std::vector<std::string> &inputFields,
                                          const std::vector<std::string> &outputFields)
        = 0;
    virtual AggFunc *createNormalFunction(const std::vector<table::ValueType> &inputTypes,
                                          const std::vector<std::string> &inputFields,
                                          const std::vector<std::string> &outputFields)
        = 0;

protected:
    bool initFunctionModel(const std::string &accTypes,
                           const std::string &paramTypes,
                           const std::string &returnTypes,
                           const autil::legacy::json::JsonMap &properties);

private:
    static bool parseIquanParamDef(const std::string &params,
                                   std::vector<iquan::ParamTypeDef> &paramsDef);

private:
    iquan::FunctionModel _functionModel;
};

#define REGISTER_NAVI_AGG_FUNC_INFO(AccTypes, ParamTypes, ReturnType, Properties)                  \
    do {                                                                                           \
        NAVI_KERNEL_LOG(INFO,                                                                      \
                        "register agg func info\n"                                                 \
                        "Functiom Name = %s\nAcc Types = %s\n"                                     \
                        "Param Types = %s\nReturn Type = %s\n"                                     \
                        "properties = %s\n",                                                       \
                        getAggFuncName().c_str(),                                                  \
                        AccTypes,                                                                  \
                        ParamTypes,                                                                \
                        ReturnType,                                                                \
                        ToJsonString(Properties).c_str());                                         \
        if (!initFunctionModel(AccTypes, ParamTypes, ReturnType, Properties)) {                    \
            NAVI_KERNEL_LOG(ERROR, "register agg func info failed");                               \
            return navi::EC_INIT_RESOURCE;                                                         \
        }                                                                                          \
    } while (false)

} // namespace sql
} // namespace isearch
