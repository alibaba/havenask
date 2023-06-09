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
#include "autil/HashFunctionBase.h"

#include <cstdint>
#include <iosfwd>

using namespace std;
namespace autil {

string HashFunctionBase::getFunctionName() const {
    return _hashFunction;
}

uint32_t HashFunctionBase::getHashSize() const {
    return _hashSize;
}

uint32_t HashFunctionBase::getHashId(const vector<string>& strVec) const {
    if (strVec.size() == 0) {
        return 0;
    } else {
        return getHashId(strVec[0]);
    }
}

vector<pair<uint32_t, uint32_t> > HashFunctionBase::getHashRange(uint32_t partId) const {
    if (partId >= _hashSize) {
        return {};
    }
    vector<pair<uint32_t, uint32_t> > range = {{partId, partId}};
    return range;
}

vector<pair<uint32_t, uint32_t> > HashFunctionBase::getHashRange(uint32_t partId, float ratio) const {
    return getHashRange(partId);
}

vector<pair<uint32_t, uint32_t> > HashFunctionBase::getHashRange(const vector<string>& strVec) const {
    uint32_t hashId = getHashId(strVec);
    vector<pair<uint32_t, uint32_t> > rangeVec;
    rangeVec.emplace_back(hashId, hashId);
    return rangeVec;
}

}
