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

#include "suez/turing/expression/function/FunctionManager.h"
// #include "ha3/cava/MatchDataCavaFunctionExpression.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace suez {
namespace turing {
class FunctionInterfaceCreator;
class FunctionProvider;
}
}

namespace ha3 {

class FunctionManager : public suez::turing::FunctionManager
{
public:
    FunctionManager(const suez::turing::FunctionInterfaceCreator *funcCreator,
                    suez::turing::FunctionProvider *provider,
                    autil::mem_pool::Pool *pool,
                    const suez::turing::CavaPluginManager *cavaPluginManager = nullptr);
    ~FunctionManager();
private:
    FunctionManager(const FunctionManager &);
    FunctionManager& operator=(const FunctionManager &);
private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(FunctionManager);

}
