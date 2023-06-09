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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib::index {

class IndexTermInfo : public autil::legacy::Jsonizable
{
public:
    IndexTermInfo() : indexName(""), term("") {}
    ~IndexTermInfo() {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("index", indexName, indexName);
        json.Jsonize("term", term, term);
    }

public:
    std::string indexName;
    std::string term;
};

class IndexQueryCondition : public autil::legacy::Jsonizable
{
public:
    IndexQueryCondition() = default;
    ~IndexQueryCondition() = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& GetConditionType() const { return mConditionType; }
    const std::vector<IndexTermInfo>& GetIndexTermInfos() const { return mIndexTermInfos; }

public:
    static const std::string AND_CONDITION_TYPE;
    static const std::string OR_CONDITION_TYPE;
    static const std::string TERM_CONDITION_TYPE;

private:
    std::vector<IndexTermInfo> mIndexTermInfos;
    std::string mConditionType; // AND OR TERM
private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
