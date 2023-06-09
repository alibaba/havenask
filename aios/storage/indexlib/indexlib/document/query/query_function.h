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
#ifndef __INDEXLIB_QUERY_FUNCTION_H
#define __INDEXLIB_QUERY_FUNCTION_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class QueryFunction
{
public:
    // first: paramName, second: paramValue
    typedef std::pair<std::string, std::string> FunctionParam;

public:
    QueryFunction();
    QueryFunction(const std::string& funcName, const std::vector<FunctionParam>& param = std::vector<FunctionParam>());
    ~QueryFunction();

public:
    bool AddFunctionParam(const std::string& paramName, const std::string& paramValue);

    std::string ToString() const;
    bool FromString(const std::string& str);

    const std::string& GetFunctionName() const { return mFuncName; }
    const std::vector<FunctionParam>& GetFunctionParameters() const { return mFuncParams; }

    size_t GetFunctionParamNum() const { return mFuncParams.size(); }
    bool GetFunctionParam(size_t paramIndex, FunctionParam& funcParam) const
    {
        if (paramIndex >= mFuncParams.size()) {
            return false;
        }
        funcParam = mFuncParams[paramIndex];
        return true;
    }

    bool GetFunctionParam(const std::string& paramName, std::string& paramValue) const
    {
        if (paramName.empty()) {
            return false;
        }
        for (auto& param : mFuncParams) {
            if (paramName == param.first) {
                paramValue = param.second;
                return true;
            }
        }
        return false;
    }

    std::string GetFunctionKey() const
    {
        return mAliasName.empty() ? std::string("__FUNC__:") + ToString() : mAliasName;
    }

    void SetAliasField(const std::string& alias) { mAliasName = alias; }
    const std::string& GetAliasField() const { return mAliasName; }

private:
    std::string mFuncName;
    std::string mAliasName;
    std::vector<FunctionParam> mFuncParams;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(QueryFunction);
}} // namespace indexlib::document

#endif //__INDEXLIB_QUERY_FUNCTION_H
