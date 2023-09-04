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

#include <string>

namespace indexlibv2 { namespace document {

inline const std::string CMD_TAG = "CMD";
inline const std::string CMD_ADD = "add";
inline const std::string CMD_DELETE = "delete";
inline const std::string CMD_DELETE_SUB = "delete_sub";
inline const std::string CMD_UPDATE_FIELD = "update_field";
inline const std::string CMD_ALTER = "alter";
inline const std::string CMD_BULKLOAD = "bulkload";
inline const std::string CMD_SKIP = "skip";
inline const std::string CMD_CHECKPOINT = "checkpoint";
inline const std::string CMD_UNKNOWN = "unknown";

inline const std::string BUILTIN_KEY_TRACE_ID = "__HA3_DOC_TRACE_ID__";
inline const std::string DOC_FLAG_FOR_METRIC = "__DOC_FLAG_FOR_METRIC__";

}} // namespace indexlibv2::document
