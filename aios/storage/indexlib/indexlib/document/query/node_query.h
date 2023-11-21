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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/query/query_base.h"
#include "indexlib/document/query/query_function.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class NodeQuery : public QueryBase
{
public:
    NodeQuery(QueryType type = QT_UNKNOWN) : QueryBase(type), mIsFunction(false) {}

    ~NodeQuery() {}

public:
    void SetFieldName(const std::string& fieldName)
    {
        mIsFunction = false;
        mFieldName = fieldName;
    }

    const std::string& GetFieldName() const { return mFieldName; }

    void SetFunction(const QueryFunction& func)
    {
        mIsFunction = true;
        mFunction = func;
    }

    const QueryFunction& GetFunction() const { return mFunction; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            QueryBase::Jsonize(json);
            if (mIsFunction) {
                std::string funcStr = mFunction.ToString();
                json.Jsonize("function", funcStr);
            }
            if (!mFieldName.empty()) {
                json.Jsonize("field", mFieldName);
            }
        }
    }

    bool IsFunctionQuery() const { return mIsFunction; }

protected:
    std::string mFieldName;
    QueryFunction mFunction;
    bool mIsFunction;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NodeQuery);
}} // namespace indexlib::document
