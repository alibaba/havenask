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
#include "suez/turing/expression/util/TypeTransformer.h"

#include <assert.h>

#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {

TypeTransformer::TypeTransformer() {}

TypeTransformer::~TypeTransformer() {}

matchdoc::BuiltinType TypeTransformer::transform(FieldType fieldType) {
    switch (fieldType) {
    case ft_unknown:
        return vt_unknown;
    case ft_time:
        return vt_unknown;
    case ft_location:
        return vt_double;
    case ft_line:
        return vt_double;
    case ft_polygon:
        return vt_double;
    case ft_online:
        return vt_unknown;
    case ft_text:
        return vt_string;
    case ft_string:
        return vt_string;
    case ft_property:
        return vt_string;
    case ft_enum:
        return vt_int32;

    case ft_float:
        return vt_float;
    case ft_double:
        return vt_double;

    case ft_int8:
        return vt_int8;
    case ft_int16:
        return vt_int16;
    case ft_integer:
        return vt_int32;
    case ft_long:
        return vt_int64;

    case ft_uint8:
        return vt_uint8;
    case ft_uint16:
        return vt_uint16;
    case ft_uint32:
        return vt_uint32;
    case ft_uint64:
        return vt_uint64;

    default:;
    }
    assert(false);
    return vt_unknown;
}

FieldType TypeTransformer::transform(std::string fieldTypeStr) {
    if (fieldTypeStr == "ft_unknown") {
        return ft_unknown;
    } else if (fieldTypeStr == "ft_text") {
        return ft_text;
    } else if (fieldTypeStr == "ft_string") {
        return ft_string;
    } else if (fieldTypeStr == "ft_enum") {
        return ft_enum;
    } else if (fieldTypeStr == "ft_integer") {
        return ft_integer;
    } else if (fieldTypeStr == "ft_float") {
        return ft_float;
    } else if (fieldTypeStr == "ft_long") {
        return ft_long;
    } else if (fieldTypeStr == "ft_time") {
        return ft_time;
    } else if (fieldTypeStr == "ft_location") {
        return ft_location;
    } else if (fieldTypeStr == "ft_polygon") {
        return ft_polygon;
    } else if (fieldTypeStr == "ft_line") {
        return ft_line;
    } else if (fieldTypeStr == "ft_online") {
        return ft_online;
    } else if (fieldTypeStr == "ft_property") {
        return ft_property;
    } else {
        return ft_unknown;
    }
}

FieldType TypeTransformer::transform(matchdoc::BuiltinType builtinType) {
    switch (builtinType) {
    case matchdoc::bt_int8:
        return ft_int8;
    case matchdoc::bt_int16:
        return ft_int16;
    case matchdoc::bt_int32:
        return ft_int32;
    case matchdoc::bt_int64:
        return ft_int64;
    case matchdoc::bt_uint8:
        return ft_uint8;
    case matchdoc::bt_uint16:
        return ft_uint16;
    case matchdoc::bt_uint32:
        return ft_uint32;
    case matchdoc::bt_uint64:
        return ft_uint64;
    case matchdoc::bt_double:
        return ft_double;
    case matchdoc::bt_float:
        return ft_float;
    case matchdoc::bt_string:
        return ft_string;
    case matchdoc::bt_hash_128:
        return ft_hash_128;
    default:;
    }
    return ft_unknown;
}
} // namespace turing
} // namespace suez
