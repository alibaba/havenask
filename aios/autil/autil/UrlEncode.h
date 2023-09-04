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

namespace autil {
class UrlEncode {
public:
    static int encode(const char *str, const int strSize, char *result, const int resultSize);
    static int decode(const char *str, const int strSize, char *result, const int resultSize);
    static std::string decode(const std::string &s);
    static std::string encode(const std::string &s);
};

} // namespace autil
