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
#ifndef __INDEXLIB_BUILDIN_MATCH_FUNCTION_FACTORY_H
#define __INDEXLIB_BUILDIN_MATCH_FUNCTION_FACTORY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater/match_function.h"
#include "indexlib/index/segment_metrics_updater/time_to_now_function.h"
#include "indexlib/index_define.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib { namespace index {

class BuildinMatchFunctionFactory
{
public:
    BuildinMatchFunctionFactory() {}
    ~BuildinMatchFunctionFactory() {}

public:
    template <typename T1, typename T2>
    static std::unique_ptr<MatchFunction> CreateFunction(const std::string& functionName,
                                                         const util::KeyValueMap& param);

private:
    template <typename T1, typename T2>
    static TimeToNowFunction<T1, T2>* CreateTimeToNowFunction();

private:
    IE_LOG_DECLARE();
};

template <typename T1, typename T2>
std::unique_ptr<MatchFunction> BuildinMatchFunctionFactory::CreateFunction(const std::string& funcName,
                                                                           const util::KeyValueMap& param)
{
    std::unique_ptr<MatchFunction> ret;
    if (funcName == TIME_TO_NOW_FUNCTION) {
        TimeToNowFunction<T1, T2>* func = CreateTimeToNowFunction<T1, T2>();
        if (!func->Init(param)) {
            IE_LOG(ERROR, "function [%s] init failed", funcName.c_str());
            return nullptr;
        }
        ret.reset(func);
        return ret;
    }
    IE_LOG(ERROR, "not support function [%s]", funcName.c_str());
    assert(false);
    return nullptr;
}

template <typename T1, typename T2>
TimeToNowFunction<T1, T2>* BuildinMatchFunctionFactory::CreateTimeToNowFunction()
{
    return new TimeToNowFunction<T1, T2>();
}

template <>
inline TimeToNowFunction<std::string, std::string>*
BuildinMatchFunctionFactory::CreateTimeToNowFunction<std::string, std::string>()
{
    assert(false);
    return nullptr;
}

DEFINE_SHARED_PTR(BuildinMatchFunctionFactory);
}} // namespace indexlib::index

#endif //__INDEXLIB_BUILDIN_MATCH_FUNCTION_FACTORY_H
