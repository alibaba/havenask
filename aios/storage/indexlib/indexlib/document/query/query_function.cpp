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
#include "indexlib/document/query/query_function.h"

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/UrlEncode.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, QueryFunction);

QueryFunction::QueryFunction() {}

QueryFunction::QueryFunction(const string& funcName, const vector<FunctionParam>& param)
    : mFuncName(funcName)
    , mFuncParams(param)
{
}

QueryFunction::~QueryFunction() {}

bool QueryFunction::AddFunctionParam(const string& paramName, const string& paramValue)
{
    if (paramValue.empty()) {
        AUTIL_LOG(ERROR, "param value for param name [%s] is null!", paramName.c_str());
        return false;
    }

    if (!paramName.empty()) {
        string value;
        if (GetFunctionParam(paramName, value)) {
            AUTIL_LOG(ERROR, "duplicate param name [%s] exist!", paramName.c_str());
            return false;
        }
    }
    mFuncParams.push_back(make_pair(paramName, paramValue));
    return true;
}

string QueryFunction::ToString() const
{
    string ret = mFuncName + "(";
    for (size_t i = 0; i < mFuncParams.size(); i++) {
        if (!mFuncParams[i].first.empty()) {
            ret += mFuncParams[i].first;
            ret += "=";
        }
        ret += UrlEncode::encode(mFuncParams[i].second);
        if (i != mFuncParams.size() - 1) {
            ret += ",";
        }
    }
    ret += ")";
    return ret;
}

bool QueryFunction::FromString(const std::string& input)
{
    string str = input;
    StringUtil::trim(str);

    if (*str.rbegin() != ')') {
        AUTIL_LOG(ERROR, "invalid function [%s]: not end with )", str.c_str());
        return false;
    }

    auto ret = str.find("(");
    if (ret == 0 || ret == string::npos) {
        AUTIL_LOG(ERROR, "invalid function [%s].", str.c_str());
        return false;
    }
    mFuncName = str.substr(0, ret);
    string paramStr = str.substr(ret + 1, str.length() - ret - 2);
    vector<string> tmp = StringTokenizer::tokenize(StringView(paramStr), ",");
    for (auto& it : tmp) {
        vector<string> kvPair = StringTokenizer::tokenize(StringView(it), "=");
        if (kvPair.size() == 1 && AddFunctionParam("", UrlEncode::decode(kvPair[0]))) {
            continue;
        }
        if (kvPair.size() == 2 && AddFunctionParam(kvPair[0], UrlEncode::decode(kvPair[1]))) {
            continue;
        }
        AUTIL_LOG(ERROR, "invalid single parameter [%s]", it.c_str());
        return false;
    }
    return true;
}
}} // namespace indexlib::document
