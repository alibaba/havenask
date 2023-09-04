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
#include "suez/turing/expression/function/BuildInFunctionFactory.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "suez/turing/expression/function/BinaryFuncInterface.h"
#include "suez/turing/expression/function/HaInFuncInterface.h"
#include "suez/turing/expression/function/HammingFuncInterface.h"
#include "suez/turing/expression/function/InFuncInterface.h"
#include "suez/turing/expression/function/ParamFromQuery.h"
#include "suez/turing/expression/function/StringFuncInterface.h"
#include "suez/turing/expression/function/SubFuncInterface.h"
#include "suez/turing/expression/function/TimeFuncInterface.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, BuildInFunctionFactory);

BuildInFunctionFactory::BuildInFunctionFactory() {}

BuildInFunctionFactory::~BuildInFunctionFactory() {}

bool BuildInFunctionFactory::registeFunctions() {
    REGISTE_FUNCTION_CREATOR(HaInFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(NotHaInFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(InFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(NotInFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(SubMinFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(SubMaxFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(SubCountFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(SubSumFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(SubJoinFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(SubFirstFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(TimeFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(StringReplaceFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(ToStringFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(PowFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(LogFuncInterfaceCreatorImpl);
    REGISTE_FUNCTION_CREATOR(HammingFuncInterfaceCreatorImpl);
    PARAM_FROM_QUERY_MACRO_HELPER(PARAM_FROM_QUERY_REGISTE_FUNCTION);
    return true;
}

} // namespace turing
} // namespace suez
