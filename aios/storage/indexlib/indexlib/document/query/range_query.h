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
#ifndef __INDEXLIB_RANGE_QUERY_H
#define __INDEXLIB_RANGE_QUERY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/query/node_query.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class RangeQuery : public NodeQuery
{
public:
    enum RangeValueType {
        RVT_UINT = 0,
        RVT_INT = 1,
        RVT_DOUBLE = 2,
    };

    union RangeValue {
        int64_t intValue;
        uint64_t uintValue;
        double doubleValue;
    };

    struct RangeInfo {
        std::string leftStr;
        std::string rightStr;
        RangeValue leftValue;
        RangeValue rightValue;
        bool leftInclude = true;
        bool rightInclude = true;
        RangeValueType valueType;
    };

public:
    RangeQuery() : NodeQuery(QT_RANGE) {}

    ~RangeQuery() {}

public:
    void Init(const std::string& fieldName, const std::string& fieldRangeStr)
    {
        NodeQuery::SetFieldName(fieldName);
        InitRangeInfo(fieldRangeStr);
    }

    void Init(const QueryFunction& function, const std::string& fieldRangeStr)
    {
        NodeQuery::SetFunction(function);
        InitRangeInfo(fieldRangeStr);
        if (!function.GetAliasField().empty()) {
            mFieldName = function.GetAliasField();
        }
    }

    const RangeInfo& GetRangeInfo() const { return mRangeInfo; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        NodeQuery::Jsonize(json);
        std::string rangeInfoStr = RangeInfoToString(mRangeInfo);
        json.Jsonize("value", rangeInfoStr);
    }

    static RangeValueType GetValueType(const std::string& str);

private:
    void InitRangeInfo(const std::string& fieldRangeStr);
    static bool ParseRangeString(const std::string& str, std::string& leftStr, std::string& rightStr, bool& includeLeft,
                                 bool& includeRight);

    static std::string RangeInfoToString(const RangeInfo& rangeInfo);

    template <typename T>
    T GetValue(const std::string& valueStr);

private:
    RangeInfo mRangeInfo;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeQuery);
}} // namespace indexlib::document

#endif //__INDEXLIB_RANGE_QUERY_H
