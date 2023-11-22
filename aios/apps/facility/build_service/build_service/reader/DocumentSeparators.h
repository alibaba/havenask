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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

static const std::string RAW_DOCUMENT_FORMAT = "document_format";
static const std::string RAW_DOCUMENT_SEP_PREFIX = "separator_prefix";
static const std::string RAW_DOCUMENT_SEP_SUFFIX = "separator_suffix";
static const std::string RAW_DOCUMENT_FIELD_SEP = "field_separator";
static const std::string RAW_DOCUMENT_KV_SEP = "kv_separator";

// ha3 document format
static const std::string RAW_DOCUMENT_HA3_DOCUMENT_FORMAT = "ha3";
static const std::string RAW_DOCUMENT_HA3_SEP_PREFIX = "";
static const std::string RAW_DOCUMENT_HA3_SEP_SUFFIX = "\x1E\n"; // \n
static const std::string RAW_DOCUMENT_HA3_FIELD_SEP = "\x1F\n";  // \n
static const std::string RAW_DOCUMENT_HA3_KV_SEP = "=";

// isearch document format
static const std::string RAW_DOCUMENT_ISEARCH_DOCUMENT_FORMAT = "isearch";
static const std::string RAW_DOCUMENT_ISEARCH_SEP_PREFIX = "<doc>\n";
static const std::string RAW_DOCUMENT_ISEARCH_SEP_SUFFIX = "</doc>\n";
static const std::string RAW_DOCUMENT_ISEARCH_FIELD_SEP = "\n";
static const std::string RAW_DOCUMENT_ISEARCH_KV_SEP = "=";

// binary document format
static const std::string RAW_DOCUMENT_BINARY_LENGTH = "length";

static const std::string RAW_DOCUMENT_FORMAT_SELF_EXPLAIN = "self_explain";
static const std::string DOC_STRING_FIELD_NAME = "raw_doc_field_name";
static const std::string DOC_STRING_FIELD_NAME_DEFAULT = "data";

static const std::string RAW_DOCUMENT_FORMAT_CUSTOMIZED = "customized";
static const std::string RAW_DOCUMENT_FORMAT_SWIFT_FILED_FILTER = "swift_field_filter";
static const std::string RAW_DOCUMENT_FORMAT_SWIFT_USE_SCHEMA = "swift_use_schema";
static const std::string RAW_DOCUMENT_FORMAT_INDEXLIB_PARSER = "indexlib_parser";
static const std::string RAW_DOCUMENT_FORMAT_HOLOGRES = "hologres";

}} // namespace build_service::reader
