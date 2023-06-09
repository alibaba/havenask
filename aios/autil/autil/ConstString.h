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

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <iosfwd>
#include <string>

#include "autil/Span.h"

namespace autil {

namespace detail {

template <typename Allocator>
StringView MakeCStringImpl(const char *data, std::size_t size, Allocator *allocator) {
    // allocate one more byte '\0', but size not change
    // it makes StringView easier to debug
    // example: data = "abc", size = 3 => _data="abc\0",_size=3
    // then you print _data then show "abc" bur data (withou \0) not
    char *buf = (char*)allocator->allocate(size + 1);
    memcpy(buf, data, size);
    buf[size] = '\0';
    return {buf, size};
}

}

template <typename Allocator>
StringView MakeCString(const char *data, Allocator *allocator) {
    return detail::MakeCStringImpl(data, ::strlen(data), allocator);
}

template <typename Allocator>
StringView MakeCString(const char *data, std::size_t size, Allocator *allocator) {
    return detail::MakeCStringImpl(data, size, allocator);
}

template <typename Allocator>
StringView MakeCString(const StringView &str, Allocator *allocator) {
    return detail::MakeCStringImpl(str.data(), str.size(), allocator);
}

}
