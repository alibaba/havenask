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
#include "autil/MurmurHash3Function.h"

#include <algorithm>
#include <cstddef>

#include "autil/Log.h"
#include "autil/MurmurHash3.h"
#include "autil/StringUtil.h"

using namespace std;

namespace autil {

AUTIL_DECLARE_AND_SETUP_LOGGER(autil, MurmurHash3Function);

MurmurHash3Function::MurmurHash3Function(const std::string &hashFunction,
                                         const std::map<std::string, std::string> &params,
                                         uint32_t partitionCount)
    : HashFunctionBase(hashFunction, partitionCount), _params(params), _seed(DEFAULT_HASH_SEED) {}

bool MurmurHash3Function::init() {
    auto it = _params.find("seed");
    if (it != _params.end()) {
        if (!StringUtil::fromString(it->second, _seed)) {
            AUTIL_LOG(WARN, "parse seed [%s] failed", it->second.c_str());
            return false;
        }
    }
    return true;
}

uint32_t MurmurHash3Function::getHashId(const std::string &str) const {
    if (_hashSize == 0) {
        return 0;
    }

    uint32_t h = 0;
    MurmurHash3_x86_32(str.c_str(), str.length(), _seed, &h);
    return h % _hashSize;
}

} // namespace autil
