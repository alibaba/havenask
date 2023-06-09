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
#include "indexlib/index/normal/attribute/format/single_value_null_attr_formatter.h"

namespace indexlib { namespace index {

uint32_t SingleEncodedNullValue::NULL_FIELD_BITMAP_SIZE = 64;
int16_t SingleEncodedNullValue::ENCODED_FP16_NULL_VALUE = std::numeric_limits<int16_t>::min();
int8_t SingleEncodedNullValue::ENCODED_FP8_NULL_VALUE = std::numeric_limits<int8_t>::min();
uint32_t SingleEncodedNullValue::BITMAP_HEADER_SIZE = sizeof(uint64_t);
}} // namespace indexlib::index
