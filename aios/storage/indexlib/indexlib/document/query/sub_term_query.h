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
#ifndef __INDEXLIB_SUB_TERM_QUERY_H
#define __INDEXLIB_SUB_TERM_QUERY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/query/node_query.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class SubTermQuery : public NodeQuery
{
public:
    SubTermQuery() : NodeQuery(QT_SUB_TERM), mDelimeter(" ") {}

    ~SubTermQuery() {}

public:
    void Init(const std::string& fieldName, const std::string& subValue, const std::string& delimeter = " ")
    {
        NodeQuery::SetFieldName(fieldName);
        mSubValue = subValue;
        mDelimeter = delimeter;
    }

    void Init(const QueryFunction& function, const std::string& subValue, const std::string& delimeter = " ")
    {
        NodeQuery::SetFunction(function);
        mSubValue = subValue;
        mDelimeter = delimeter;
        if (!function.GetAliasField().empty()) {
            mFieldName = function.GetAliasField();
        }
    }

    const std::string& GetSubValue() const { return mSubValue; }
    const std::string& GetDelimeter() const { return mDelimeter; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        NodeQuery::Jsonize(json);
        json.Jsonize("value", mSubValue);
        json.Jsonize("delimeter", mDelimeter, mDelimeter);
    }

private:
    std::string mSubValue;
    std::string mDelimeter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubTermQuery);
}} // namespace indexlib::document

#endif //__INDEXLIB_SUB_TERM_QUERY_H
