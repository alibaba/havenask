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

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

enum QueryType {
    QT_TERM = 0,     // string exactly match
    QT_SUB_TERM = 1, // sub item string exactly match
    QT_PATTERN = 2,  // string regular expression match
    QT_RANGE = 3,    // number match by range
    QT_AND = 4,
    QT_OR = 5,
    QT_NOT = 6,
    QT_UNKNOWN = 7,
};

class QueryBase : public autil::legacy::Jsonizable
{
public:
    QueryBase(QueryType type = QT_UNKNOWN) : mType(type) {}

    virtual ~QueryBase() {}

public:
    QueryType GetType() const { return mType; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            std::string typeStr = QueryTypeToStr(mType);
            json.Jsonize("type", typeStr);
        }
    }

public:
    static const char* QueryTypeToStr(QueryType type)
    {
        switch (type) {
        case QT_TERM:
            return "TERM";
        case QT_SUB_TERM:
            return "SUB_TERM";
        case QT_PATTERN:
            return "PATTERN";
        case QT_RANGE:
            return "RANGE";
        case QT_AND:
            return "AND";
        case QT_OR:
            return "OR";
        case QT_NOT:
            return "NOT";
        default:
            return "UNKNOWN";
        }
        return "UNKNOWN";
    }

    static QueryType StringToQueryType(const std::string& str)
    {
        if (str == "TERM") {
            return QT_TERM;
        }
        if (str == "SUB_TERM") {
            return QT_SUB_TERM;
        }
        if (str == "RANGE") {
            return QT_RANGE;
        }
        if (str == "PATTERN") {
            return QT_PATTERN;
        }
        if (str == "AND") {
            return QT_AND;
        }
        if (str == "OR") {
            return QT_OR;
        }
        if (str == "NOT") {
            return QT_NOT;
        }
        return QT_UNKNOWN;
    }

protected:
    QueryType mType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(QueryBase);
}} // namespace indexlib::document
