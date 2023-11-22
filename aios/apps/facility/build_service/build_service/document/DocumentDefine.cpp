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
#include "build_service/document/DocumentDefine.h"

#include "indexlib/document/RawDocumentDefine.h"
#include "indexlib/document/raw_document/raw_document_define.h"

namespace build_service { namespace document {

const std::string CMD_SEPARATOR = "\x1E\n";
const std::string KEY_VALUE_SEPARATOR = "\x1F\n";
const size_t KEY_VALUE_SEPARATOR_LENGTH = KEY_VALUE_SEPARATOR.length();

const std::string& CMD_TAG = indexlib::document::CMD_TAG;
const std::string& CMD_ADD = indexlib::document::CMD_ADD;
const std::string& CMD_DELETE = indexlib::document::CMD_DELETE;
const std::string& CMD_DELETE_SUB = indexlib::document::CMD_DELETE_SUB;
const std::string& CMD_UPDATE_FIELD = indexlib::document::CMD_UPDATE_FIELD;
const std::string& CMD_SKIP = indexlib::document::CMD_SKIP;

const std::string HA_RESERVED_MODIFY_FIELDS = "ha_reserved_modify_fields";
const std::string HA_RESERVED_SUB_MODIFY_FIELDS = "ha_reserved_sub_modify_fields";
const std::string HA_RESERVED_MODIFY_FIELDS_SEPARATOR = ";";
const std::string HA_RESERVED_MODIFY_VALUES_SEPARATOR = "\x01\x02";
const std::string HA_RESERVED_MODIFY_VALUES = "ha_reserved_modify_values";
const std::string LAST_VALUE_PREFIX = "__last_value__";
const std::string HA_RESERVED_SKIP_REASON = "ha_reserved_skip_reason";
const std::string UPDATE_FAILED = "update_failed";

}} // namespace build_service::document
