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

#include "autil/Log.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FunctionInterface.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace suez {
namespace turing {
class AttributeExpression;
class FunctionInterfaceCreator;
class FunctionProvider;
class CavaFunctionModuleInfo;

class FunctionManager {
public:
    FunctionManager(const FunctionInterfaceCreator *funcCreator,
                    FunctionProvider *provider,
                    autil::mem_pool::Pool *pool,
                    const suez::turing::CavaPluginManager *cavaPluginManager = nullptr);
    virtual ~FunctionManager();

private:
    FunctionManager(const FunctionManager &);
    FunctionManager &operator=(const FunctionManager &);

public:
    AttributeExpression *createFuncExpression(const std::string &funcName,
                                              const FunctionSubExprVec &funcSubExprVec,
                                              VariableType vt,
                                              bool isMulti,
                                              bool isSubExpr);
    const FunctionInterfaceCreator *getFunctionInterfaceCreator() const { return _funcCreator; }
    FunctionProvider *getFunctionProvider() const { return _provider; }

private:
    AttributeExpression *createCavaFuncExpression(const std::string &funcName,
                                                  const FunctionSubExprVec &funcSubExprVec,
                                                  VariableType vt,
                                                  bool isMulti,
                                                  bool isSubExpr,
                                                  bool funcIsDeterministic);
    template <typename T>
    AttributeExpression *createAttrExpression(FunctionInterface *func) const;
    virtual AttributeExpression *doCreateCavaFuncExpression(const std::string &funcName,
                                                            const FunctionSubExprVec &funcSubExprVec,
                                                            VariableType vt,
                                                            bool isMulti,
                                                            CavaFunctionModuleInfo *moduleInfo,
                                                            bool prefetchConst);

private:
    const FunctionInterfaceCreator *_funcCreator = nullptr;
    FunctionProvider *_provider = nullptr;

protected:
    autil::mem_pool::Pool *_pool = nullptr;
    const suez::turing::CavaPluginManager *_cavaPluginManager = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(FunctionManager);

} // namespace turing
} // namespace suez
