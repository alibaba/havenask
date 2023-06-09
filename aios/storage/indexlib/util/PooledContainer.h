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
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/mem_pool/pool_allocator.h"

namespace indexlib::util {

template <typename Key, typename T>
using PooledMap = std::map<Key, T, std::less<Key>, autil::mem_pool::pool_allocator<std::pair<const Key, T>>>;

template <typename Key, typename T>
using PooledUnorderedMap = std::unordered_map<Key, T, std::hash<Key>, std::equal_to<Key>,
                                              autil::mem_pool::pool_allocator<std::pair<const Key, T>>>;

template <typename T>
using PooledVector = std::vector<T, autil::mem_pool::pool_allocator<T>>;

template <typename T>
using PooledString = std::basic_string<char, std::char_traits<char>, autil::mem_pool::pool_allocator<char>>;

} // namespace indexlib::util
