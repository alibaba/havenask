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

#include <memory>
#include <stdint.h>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace util {

class EncodeConverter {
public:
    EncodeConverter();
    ~EncodeConverter();

public:
    static int32_t utf8ToUtf16(const char *in, int32_t length, uint16_t *out);
    static int32_t utf16ToUtf8(const uint16_t *in, int32_t length, char *out);
    static int32_t utf8ToUtf16Len(const char *in, int32_t length);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<EncodeConverter> EncodeConverterPtr;

} // namespace util
} // namespace isearch
