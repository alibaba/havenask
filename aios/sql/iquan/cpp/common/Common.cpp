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
#include "iquan/common/Common.h"

const std::string IQUAN_ATTRS = "attrs";
const std::string IQUAN_JSON = "json";
const std::string IQUAN_TRUE = "true";
const std::string IQUAN_FALSE = "false";

// dynamic params
const std::string IQUAN_DYNAMIC_PARAMS_PREFIX = "[dynamic_params[[?";
const std::string IQUAN_DYNAMIC_PARAMS_SUFFIX = "]]dynamic_params]";
const std::string IQUAN_REPLACE_PARAMS_PREFIX = "[replace_params[[";
const std::string IQUAN_REPLACE_PARAMS_SUFFIX = "]]replace_params]";

const std::string IQUAN_DYNAMIC_PARAMS_SEPARATOR = "#";

const size_t IQUAN_DYNAMIC_PARAMS_MIN_SIZE = IQUAN_DYNAMIC_PARAMS_PREFIX.size()
                                             + IQUAN_DYNAMIC_PARAMS_SEPARATOR.size()
                                             + IQUAN_DYNAMIC_PARAMS_SUFFIX.size();
const size_t IQUAN_REPLACE_PARAMS_MIN_SIZE = IQUAN_REPLACE_PARAMS_PREFIX.size()
                                             + IQUAN_DYNAMIC_PARAMS_SEPARATOR.size()
                                             + IQUAN_REPLACE_PARAMS_SUFFIX.size();

const std::string IQUAN_DYNAMIC_PARAMS_SIMD_PADDING(128, '\0');

// hint params
const std::string IQUAN_HINT_PARAMS_PREFIX = "[hint_params[";
const std::string IQUAN_HINT_PARAMS_SUFFIX = "]hint_params]";
const size_t IQUAN_HINT_PARAMS_MIN_SIZE
    = IQUAN_HINT_PARAMS_PREFIX.size() + IQUAN_HINT_PARAMS_SUFFIX.size();
