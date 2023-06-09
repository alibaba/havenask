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
#include "indexlib/document/query/range_query.h"

#include "autil/StringTokenizer.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, RangeQuery);

template <typename T>
T RangeQuery::GetValue(const string& valueStr)
{
    T value {};
    if (!StringUtil::fromString(valueStr, value)) {
        INDEXLIB_FATAL_ERROR(BadParameter, "invalid range value str [%s]", valueStr.c_str());
    }
    return value;
}

void RangeQuery::InitRangeInfo(const string& fieldRangeStr)
{
    if (!ParseRangeString(fieldRangeStr, mRangeInfo.leftStr, mRangeInfo.rightStr, mRangeInfo.leftInclude,
                          mRangeInfo.rightInclude)) {
        INDEXLIB_FATAL_ERROR(BadParameter, "invalid range string [%s]", fieldRangeStr.c_str());
    }

    int leftValueType = (int)GetValueType(mRangeInfo.leftStr);
    int rightValueType = (int)GetValueType(mRangeInfo.rightStr);
    mRangeInfo.valueType = (RangeValueType)max(leftValueType, rightValueType);
    if (mRangeInfo.valueType == RVT_INT) {
        mRangeInfo.leftValue.intValue = GetValue<int64_t>(mRangeInfo.leftStr);
        mRangeInfo.rightValue.intValue = GetValue<int64_t>(mRangeInfo.rightStr);
    } else if (mRangeInfo.valueType == RVT_UINT) {
        mRangeInfo.leftValue.uintValue = GetValue<uint64_t>(mRangeInfo.leftStr);
        mRangeInfo.rightValue.uintValue = GetValue<uint64_t>(mRangeInfo.rightStr);
    } else {
        assert(mRangeInfo.valueType == RVT_DOUBLE);
        mRangeInfo.leftValue.doubleValue = GetValue<double>(mRangeInfo.leftStr);
        mRangeInfo.rightValue.doubleValue = GetValue<double>(mRangeInfo.rightStr);
    }
}

bool RangeQuery::ParseRangeString(const string& str, string& leftStr, string& rightStr, bool& includeLeft,
                                  bool& includeRight)
{
    string rangeValue = str;
    int len = str.length();
    if (str[0] == '[') {
        includeLeft = true;
        if (str[len - 1] == ')') {
            includeRight = false;
        } else if (str[len - 1] == ']') {
            includeRight = true;
        } else {
            return false;
        }
        rangeValue = str.substr(1, len - 2);
    } else if (str[0] == '(') {
        includeLeft = false;
        if (str[len - 1] == ')') {
            includeRight = false;
        } else if (str[len - 1] == ']') {
            includeRight = true;
        } else {
            return false;
        }
        rangeValue = str.substr(1, len - 2);
    } else {
        includeLeft = true;
        includeRight = true;
    }

    StringTokenizer st(rangeValue, ",", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    if (st.getNumTokens() == 2) {
        leftStr = st[0];
        rightStr = st[1];
        return true;
    }
    if (st.getNumTokens() == 1) {
        leftStr = st[0];
        rightStr = st[0];
        return true;
    }
    return false;
}

string RangeQuery::RangeInfoToString(const RangeInfo& rangeInfo)
{
    if (rangeInfo.leftInclude && rangeInfo.rightInclude && rangeInfo.leftStr == rangeInfo.rightStr) {
        return rangeInfo.leftStr;
    }

    string ret;
    ret += rangeInfo.leftInclude ? "[" : "(";
    ret += rangeInfo.leftStr;
    ret += ",";
    ret += rangeInfo.rightStr;
    ret += rangeInfo.rightInclude ? "]" : ")";
    return ret;
}

RangeQuery::RangeValueType RangeQuery::GetValueType(const string& str)
{
    if (str.find(".") != string::npos) {
        return RVT_DOUBLE;
    }
    if (str[0] == '-') {
        return RVT_INT;
    }
    return RVT_UINT;
}
}} // namespace indexlib::document
