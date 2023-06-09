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
#include "build_service/reader/Separator.h"

using namespace std;
namespace build_service { namespace reader {

Separator::Separator(const string& sep)
{
    _sep = sep;
    uint32_t sepLen = sep.size();
    for (int32_t i = 0; i < 256; i++) {
        _next[i] = 1;
    }
    for (uint32_t i = 0; i < sepLen; i++) {
        _next[(unsigned char)(sep[i])] = 0 - i;
    }
}

Separator::~Separator() {}

}} // namespace build_service::reader
