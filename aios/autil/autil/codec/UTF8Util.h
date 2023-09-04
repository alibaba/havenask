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

#include <stddef.h>
#include <string>

#include "autil/Log.h"

namespace autil {
namespace codec {

class UTF8Util {
private:
    UTF8Util();
    ~UTF8Util();
    UTF8Util(const UTF8Util &);
    UTF8Util &operator=(const UTF8Util &);

public:
    static std::string getNextCharUTF8(const std::string &str, size_t start);
    static bool getNextCharUTF8(const char *str, size_t start, size_t end, size_t &len);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace codec
} // namespace autil
