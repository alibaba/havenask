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
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"

namespace ha3 {

std::map<std::string, CAVA_FIELD_TYPE> CavaFieldTypeHelper::_name2Type = {
    {"boolean", CAVA_FIELD_TYPE::ct_boolean}, {"byte", CAVA_FIELD_TYPE::ct_byte},
    {"short", CAVA_FIELD_TYPE::ct_short},     {"int", CAVA_FIELD_TYPE::ct_int},
    {"long", CAVA_FIELD_TYPE::ct_long},       {"ubyte", CAVA_FIELD_TYPE::ct_ubyte},
    {"ushort", CAVA_FIELD_TYPE::ct_ushort},   {"uint", CAVA_FIELD_TYPE::ct_uint},
    {"ulong", CAVA_FIELD_TYPE::ct_ulong},     {"float", CAVA_FIELD_TYPE::ct_float},
    {"double", CAVA_FIELD_TYPE::ct_double},   {"MChar", CAVA_FIELD_TYPE::ct_MChar},
    {"MInt8", CAVA_FIELD_TYPE::ct_MInt8},     {"MInt16", CAVA_FIELD_TYPE::ct_MInt16},
    {"MInt32", CAVA_FIELD_TYPE::ct_MInt32},   {"MInt64", CAVA_FIELD_TYPE::ct_MInt64},
    {"MUInt8", CAVA_FIELD_TYPE::ct_MUInt8},   {"MUInt16", CAVA_FIELD_TYPE::ct_MUInt16},
    {"MUInt32", CAVA_FIELD_TYPE::ct_MUInt32}, {"MUInt64", CAVA_FIELD_TYPE::ct_MUInt64},
    {"MFloat", CAVA_FIELD_TYPE::ct_MFloat},   {"MDouble", CAVA_FIELD_TYPE::ct_MDouble},
    {"MString", CAVA_FIELD_TYPE::ct_MString}};

std::map<std::string, std::pair<matchdoc::BuiltinType, bool>> CavaFieldTypeHelper::_name2TypePair = {
    {"boolean", std::make_pair(matchdoc::bt_bool, false)},  {"byte", std::make_pair(matchdoc::bt_int8, false)},
    {"short", std::make_pair(matchdoc::bt_int16, false)},   {"int", std::make_pair(matchdoc::bt_int32, false)},
    {"long", std::make_pair(matchdoc::bt_int64, false)},    {"ubyte", std::make_pair(matchdoc::bt_uint8, false)},
    {"ushort", std::make_pair(matchdoc::bt_uint16, false)}, {"uint", std::make_pair(matchdoc::bt_uint32, false)},
    {"ulong", std::make_pair(matchdoc::bt_uint64, false)},  {"float", std::make_pair(matchdoc::bt_float, false)},
    {"double", std::make_pair(matchdoc::bt_double, false)}, {"MChar", std::make_pair(matchdoc::bt_string, false)},
    {"MInt8", std::make_pair(matchdoc::bt_int8, true)},     {"MInt16", std::make_pair(matchdoc::bt_int16, true)},
    {"MInt32", std::make_pair(matchdoc::bt_int32, true)},   {"MInt64", std::make_pair(matchdoc::bt_int64, true)},
    {"MUInt8", std::make_pair(matchdoc::bt_uint8, true)},   {"MUInt16", std::make_pair(matchdoc::bt_uint16, true)},
    {"MUInt32", std::make_pair(matchdoc::bt_uint32, true)}, {"MUInt64", std::make_pair(matchdoc::bt_uint64, true)},
    {"MFloat", std::make_pair(matchdoc::bt_float, true)},   {"MDouble", std::make_pair(matchdoc::bt_double, true)},
    {"MString", std::make_pair(matchdoc::bt_string, true)}};

} // namespace ha3
