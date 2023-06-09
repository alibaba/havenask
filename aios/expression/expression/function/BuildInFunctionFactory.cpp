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
#include "expression/function/BuildInFunctionFactory.h"
#include "expression/function/InFuncInterface.h"
#include "expression/function/InStringFuncInterface.h"
#include "expression/function/StrcatFuncInterface.h"
#include "expression/function/BuiltInSubFunctionCreator.h"
#include "expression/function/SortFuncInterface.h"

using namespace std;

namespace expression {

BuildInFunctionFactory::BuildInFunctionFactory() { 
}

BuildInFunctionFactory::~BuildInFunctionFactory() { 
}

bool BuildInFunctionFactory::registerFunctions() {
    AIOS_REGISTER_FUNCTION_CREATOR(InFuncInterfaceCreator);
    AIOS_REGISTER_FUNCTION_CREATOR(NotInFuncInterfaceCreator);
    AIOS_REGISTER_FUNCTION_CREATOR(InStringFuncInterfaceCreator);
    AIOS_REGISTER_FUNCTION_CREATOR(NotInStringFuncInterfaceCreator);
    AIOS_REGISTER_FUNCTION_CREATOR(SubMinFuncInterfaceCreatorImpl);
    AIOS_REGISTER_FUNCTION_CREATOR(SubMaxFuncInterfaceCreatorImpl);
    AIOS_REGISTER_FUNCTION_CREATOR(SubCountFuncInterfaceCreatorImpl);
    AIOS_REGISTER_FUNCTION_CREATOR(SubSumFuncInterfaceCreatorImpl);
    AIOS_REGISTER_FUNCTION_CREATOR(SubJoinFuncInterfaceCreatorImpl);
    AIOS_REGISTER_FUNCTION_CREATOR(SubFirstFuncInterfaceCreatorImpl);
    AIOS_REGISTER_FUNCTION_CREATOR(SortFuncInterfaceCreator);
    AIOS_REGISTER_FUNCTION_CREATOR(StrcatFuncInterfaceCreator);
    return true;
}

}
