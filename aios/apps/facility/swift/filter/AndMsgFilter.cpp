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
#include "swift/filter/AndMsgFilter.h"

#include <algorithm>
#include <cstddef>

#include "autil/CommonMacros.h"

namespace swift {
namespace common {
class FieldGroupReader;
} // namespace common
} // namespace swift

using namespace std;
using namespace swift::common;

namespace swift {
namespace filter {
AUTIL_LOG_SETUP(swift, AndMsgFilter);

AndMsgFilter::AndMsgFilter() {}

AndMsgFilter::~AndMsgFilter() {
    for (size_t i = 0; i < _msgFilterVec.size(); i++) {
        DELETE_AND_SET_NULL(_msgFilterVec[i]);
    }
}

bool AndMsgFilter::init() { return true; }

bool AndMsgFilter::filterMsg(const FieldGroupReader &fieldGroupReader) const {
    for (size_t i = 0; i < _msgFilterVec.size(); i++) {
        if (!_msgFilterVec[i]->filterMsg(fieldGroupReader)) {
            return false;
        }
    }
    return true;
}

void AndMsgFilter::addMsgFilter(const MsgFilter *msgFilter) {
    if (msgFilter) {
        _msgFilterVec.push_back(msgFilter);
    }
}

} // namespace filter
} // namespace swift
