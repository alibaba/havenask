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
#include "autil/DefaultHashFunction.h"

#include <cstddef>

#include "autil/HashAlgorithm.h"

using namespace std;
namespace autil {

int DefaultHashFunction::hashString(const string &str) const {
    int h = (int)HashAlgorithm::hashString(str.c_str(), str.length(), 0);
    return h;
}

uint32_t DefaultHashFunction::getHashId(const std::string &str) const {
    uint32_t h = (uint32_t)HashAlgorithm::hashString(str.c_str(), str.length(), 0);
    return h % _hashSize;
}

uint32_t DefaultHashFunction::getHashId(const char *buf, size_t len) const {
    uint32_t h = (uint32_t)HashAlgorithm::hashString(buf, len, 0);
    return h % _hashSize;
}

} // namespace autil
