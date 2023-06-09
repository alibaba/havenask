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

namespace indexlib::index {

static const uint32_t DICTIONARY_MAGIC_NUMBER = 0x98765432;
static const uint32_t DICTIONARY_WITH_NULLTERM_MAGIC_NUMBER = 0x98765433;
static const uint32_t ITEM_COUNT_PER_BLOCK = 128;

template <typename KeyType>
struct DictionaryItemTyped {
    KeyType first;
    dictvalue_t second;
} __attribute__((packed));

template <typename KeyType>
struct HashDictionaryItemTyped {
    KeyType key = 0;
    uint32_t next = std::numeric_limits<uint32_t>::max();
    dictvalue_t value = 0;
} __attribute__((packed));

} // namespace indexlib::index
