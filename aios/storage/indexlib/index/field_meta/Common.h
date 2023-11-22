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
namespace indexlib::index {
inline const std::string FIELD_META_INDEX_TYPE_STR = "field_meta";
inline const std::string FIELD_META_INDEX_PATH = "field_meta";
inline const std::string MIN_MAX_META_STR = "MinMax";
inline const std::string MIN_MAX_META_FILE_NAME = "min_max_meta";
inline const std::string HISTOGRAM_META_STR = "Histogram";
inline const std::string HISTOGRAM_META_FILE_NAME = "histogram_meta";
inline const std::string DATA_STATISTICS_META_STR = "DataStatistics";
inline const std::string DATA_STATISTICS_META_FILE_NAME = "data_statistics_meta";
inline const std::string FIELD_TOKEN_COUNT_META_STR = "FieldTokenCount";
inline const std::string FIELD_TOKEN_COUNT_META_FILE_NAME = "field_token_count_meta";
inline const std::string SOURCE_TOKEN_COUNT_STORE_SUFFIX = "__token_count__";

inline const std::string SOURCE_FIELD_READER_PARAM = "source_field_reader_param";

} // namespace indexlib::index
