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

namespace indexlibv2::framework {

static constexpr const char* ALTER_TABLE_TASK_TYPE = "alter_table";
static constexpr const char* BULKLOAD_TASK_TYPE = "bulkload";
static constexpr const char* PARAM_BULKLOAD_ID = "bulkload_id";
static constexpr const char* PARAM_EXTERNAL_FILES = "external_files";
static constexpr const char* PARAM_IMPORT_EXTERNAL_FILE_OPTIONS = "import_external_file_options";
static constexpr const char* PARAM_LAST_SEQUENCE_NUMBER = "last_sequence_number";
static constexpr const char* INDEX_TASK_CONFIG_IN_PARAM = "param_index_task_config";
static constexpr const char* ACTION_KEY = "__action__";

} // namespace indexlibv2::framework
