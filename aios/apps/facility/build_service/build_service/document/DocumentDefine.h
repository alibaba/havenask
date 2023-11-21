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

#include <stddef.h>
#include <string>

namespace build_service { namespace document {

extern const std::string CMD_SEPARATOR;
extern const std::string KEY_VALUE_SEPARATOR;

constexpr char SUB_DOC_SEPARATOR = '\x03';
constexpr char TOKEN_PAYLOAD_SYMBOL = '\x1C';
constexpr char PAYLOAD_FIELD_SYMBOL = '\x10';
constexpr char KEY_VALUE_EQUAL_SYMBOL = '=';
constexpr size_t MAX_DOC_LENGTH = 1024 * 1024;
extern const size_t KEY_VALUE_SEPARATOR_LENGTH;
constexpr size_t KEY_VALUE_EQUAL_SYMBOL_LENGTH = sizeof(char);
constexpr char HA_RESERVED_MODIFY_VALUES_MAGIC_HEADER = '\x1D';

extern const std::string& CMD_TAG;
extern const std::string& CMD_ADD;
extern const std::string& CMD_DELETE;
extern const std::string& CMD_DELETE_SUB;
extern const std::string& CMD_UPDATE_FIELD;
extern const std::string& CMD_SKIP;

extern const std::string HA_RESERVED_MODIFY_FIELDS;
extern const std::string HA_RESERVED_SUB_MODIFY_FIELDS;
extern const std::string HA_RESERVED_MODIFY_FIELDS_SEPARATOR;
extern const std::string HA_RESERVED_MODIFY_VALUES_SEPARATOR;
extern const std::string HA_RESERVED_MODIFY_VALUES;
extern const std::string LAST_VALUE_PREFIX;
extern const std::string HA_RESERVED_SKIP_REASON;
extern const std::string UPDATE_FAILED;

}} // namespace build_service::document
