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
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "build_service/plugin/ModuleFactory.h"
#include "ha3/sql/common/Log.h"
#include "iquan/common/catalog/FunctionModel.h"

namespace iquan {
class ParamTypeDef;
} // namespace iquan
namespace isearch {
namespace sql {
class AggFuncCreatorR;
} // namespace sql
} // namespace isearch
namespace autil {
class InterfaceManager;
}
namespace isearch {
namespace sql {


#define REGISTER_AGG_FUNC_INFO(AggFuncName, AccTypes, ParamTypes, ReturnType, Properties) \
    do {                                                                \
        SQL_LOG(INFO,                                                   \
                "register agg func info\n"                              \
                "Functiom Name = %s\nAcc Types = %s\n"                  \
                "Param Types = %s\nReturn Type = %s\n"                  \
                "properties = %s\n",                                    \
                AggFuncName,                                            \
                AccTypes,                                               \
                ParamTypes,                                             \
                ReturnType,                                             \
                ToJsonString(Properties).c_str());                      \
        if (!registerAggFuncInfo(AggFuncName, AccTypes, ParamTypes, ReturnType, Properties)) { \
            SQL_LOG(ERROR, "register [%s] agg func info failed", AggFuncName); \
            return false;                                               \
        }                                                               \
    } while (false)

static const std::string MODULE_SQL_AGG_FUNCTION = "_Sql_Agg_Function";

class AggFuncCreatorFactory : public build_service::plugin::ModuleFactory {
public:
    AggFuncCreatorFactory();
    virtual ~AggFuncCreatorFactory();

private:
    AggFuncCreatorFactory(const AggFuncCreatorFactory &);
    AggFuncCreatorFactory &operator=(const AggFuncCreatorFactory &);

public:
    virtual bool registerAggFunctions(autil::InterfaceManager *manager) = 0;
    virtual bool registerAggFunctionInfos() = 0;

public:
    typedef std::map<std::string, AggFuncCreatorR *> AggFuncCreatorMap;

public:
    bool registerFunctionInfos(iquan::FunctionModels &functionModels,
                               std::map<std::string, size_t> &accSize);
    void destroy() override;
    virtual std::string getName() {
        return "AggFuncCreatorFactory";
    }

protected:
    bool registerAggFuncInfo(const std::string &name,
                             const std::string &accTypes,
                             const std::string &paramTypes,
                             const std::string &returnTypes,
                             const autil::legacy::json::JsonMap &properties);
    bool registerAccSize(const std::string &name, size_t i);

private:
    static bool parseIquanParamDef(const std::string &params,
                                   std::vector<iquan::ParamTypeDef> &paramsDef);

protected:
    iquan::FunctionModels _functionInfosDef;
    std::map<std::string, size_t> _accSize;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggFuncCreatorFactory> AggFuncCreatorFactoryPtr;
} // namespace sql
} // namespace isearch
