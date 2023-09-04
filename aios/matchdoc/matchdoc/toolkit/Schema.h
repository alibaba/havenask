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

#include <string.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "matchdoc/ValueType.h"

namespace matchdoc {

static const std::string kAttrSchemaSep = ":";
static const std::string kFieldSchemaSep = ";";
static const std::string kMultiValSep = " ";
static const std::string kFieldValSep = ",";

inline static std::string toString(matchdoc::BuiltinType vt) {
    switch (vt) {
    case matchdoc::bt_bool:
        return "bool";
    case matchdoc::bt_int8:
        return "int8";
    case matchdoc::bt_uint8:
        return "uint8";
    case matchdoc::bt_int16:
        return "int16";
    case matchdoc::bt_uint16:
        return "uint16";
    case matchdoc::bt_int32:
        return "int32";
    case matchdoc::bt_uint32:
        return "uint32";
    case matchdoc::bt_int64:
        return "int64";
    case matchdoc::bt_uint64:
        return "uint64";
    case matchdoc::bt_float:
        return "float";
    case matchdoc::bt_double:
        return "double";
    case matchdoc::bt_string:
        return "string";
    default:
        return "unknown";
    }
}

inline static matchdoc::BuiltinType fromString(const std::string &typeStr) {
    if (!strcasecmp(typeStr.c_str(), "bool")) {
        return matchdoc::bt_bool;
    }
    if (!strcasecmp(typeStr.c_str(), "string")) {
        return matchdoc::bt_string;
    }
    if (!strcasecmp(typeStr.c_str(), "int8")) {
        return matchdoc::bt_int8;
    }
    if (!strcasecmp(typeStr.c_str(), "int16")) {
        return matchdoc::bt_int16;
    }
    if (!strcasecmp(typeStr.c_str(), "int32")) {
        return matchdoc::bt_int32;
    }
    if (!strcasecmp(typeStr.c_str(), "int64")) {
        return matchdoc::bt_int64;
    }
    if (!strcasecmp(typeStr.c_str(), "uint8")) {
        return matchdoc::bt_uint8;
    }
    if (!strcasecmp(typeStr.c_str(), "uint16")) {
        return matchdoc::bt_uint16;
    }
    if (!strcasecmp(typeStr.c_str(), "uint32")) {
        return matchdoc::bt_uint32;
    }
    if (!strcasecmp(typeStr.c_str(), "uint64")) {
        return matchdoc::bt_uint64;
    }
    if (!strcasecmp(typeStr.c_str(), "float")) {
        return matchdoc::bt_float;
    }
    if (!strcasecmp(typeStr.c_str(), "double")) {
        return matchdoc::bt_double;
    }
    return matchdoc::bt_unknown;
}

struct Field : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    std::string _name;
    matchdoc::BuiltinType _type;
    bool _isMulti;
    std::string _multiValSep;
    std::vector<std::string> _fieldVals; // each value include several docs
};

struct Doc : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool fillFields(const std::vector<std::string> &docsValue);
    std::string _name;
    std::vector<Field> _fields;
    std::vector<std::string> _docVals; // each value include several fields
    std::string _fieldValSep;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace matchdoc
