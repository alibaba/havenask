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

#include "indexlib/index/primary_key/Types.h"

namespace indexlib::index {
static constexpr const char* PRIMARY_KEY_DATA_SLICE_FILE_NAME = "slice_data";
static constexpr const char* PRIMARY_KEY_DATA_FILE_NAME = "data";
static constexpr const char* PRIMARY_KEY_ATTRIBUTE_PREFIX = "attribute";

} // namespace indexlib::index

namespace indexlibv2::index {
using indexlib::index::PRIMARY_KEY_DATA_FILE_NAME;
using indexlib::index::PRIMARY_KEY_DATA_SLICE_FILE_NAME;
} // namespace indexlibv2::index
