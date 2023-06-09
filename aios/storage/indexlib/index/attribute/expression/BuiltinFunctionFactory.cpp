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
#include "indexlib/index/attribute/expression/BuiltinFunctionFactory.h"

#include "autil/TimeUtility.h"
#include "indexlib/index/attribute/expression/BuiltinFunctionInterface.h"

namespace indexlibv2::index {

BuiltinFunctionFactory::BuiltinFunctionFactory() {}

BuiltinFunctionFactory::~BuiltinFunctionFactory() {}

bool BuiltinFunctionFactory::registerFunctions()
{
    struct AlwaysTruePolicy {
        typedef bool ResultType;
        void Init() {}
        ResultType Func() { return true; }
        static std::string FuncName() { return "always_true"; }
    };
    struct CurrentSecondPolicy {
        typedef int64_t ResultType;
        ResultType currentTimeInSeconds = -1;
        void Init() { currentTimeInSeconds = autil::TimeUtility::currentTimeInSeconds(); }
        ResultType Func() { return currentTimeInSeconds; }
        static std::string FuncName() { return "current_second"; }
    };
    AIOS_REGISTER_FUNCTION_CREATOR(BuiltinFunction<AlwaysTruePolicy>::BuiltinFunctionCreator);
    AIOS_REGISTER_FUNCTION_CREATOR(BuiltinFunction<CurrentSecondPolicy>::BuiltinFunctionCreator);
    return true;
}

} // namespace indexlibv2::index
