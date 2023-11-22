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
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class AndQuery : public QueryBase
{
public:
    AndQuery() : QueryBase(QT_AND) {}
    ~AndQuery() {}

public:
    void Init(const std::vector<QueryBasePtr>& subQuerys)
    {
        assert(subQuerys.size() > 1);
        mSubQuery = subQuerys;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        QueryBase::Jsonize(json);
        json.Jsonize("sub_query", mSubQuery);
    }

    const std::vector<QueryBasePtr>& GetSubQuery() const { return mSubQuery; }

private:
    std::vector<QueryBasePtr> mSubQuery;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AndQuery);
}} // namespace indexlib::document
