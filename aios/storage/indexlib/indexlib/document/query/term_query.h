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
#include "indexlib/document/query/node_query.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class TermQuery : public NodeQuery
{
public:
    TermQuery() : NodeQuery(QT_TERM) {}

    ~TermQuery() {}

public:
    void Init(const std::string& fieldName, const std::string& fieldValue)
    {
        NodeQuery::SetFieldName(fieldName);
        mValue = fieldValue;
    }

    void Init(const QueryFunction& function, const std::string& value)
    {
        NodeQuery::SetFunction(function);
        mValue = value;
        if (!function.GetAliasField().empty()) {
            mFieldName = function.GetAliasField();
        }
    }

    const std::string& GetValue() const { return mValue; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        NodeQuery::Jsonize(json);
        json.Jsonize("value", mValue);
    }

private:
    std::string mValue;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TermQuery);
}} // namespace indexlib::document
