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
#include "swift/filter/DescFieldFilter.h"

#include <assert.h>
#include <stddef.h>

#include "swift/filter/MsgFilter.h"
#include "swift/filter/MsgFilterCreator.h"

namespace swift {
namespace common {
class FieldGroupReader;
} // namespace common
} // namespace swift

namespace swift {
namespace filter {
AUTIL_LOG_SETUP(swift, DescFieldFilter);

DescFieldFilter::DescFieldFilter() { _msgFilter = NULL; }

DescFieldFilter::~DescFieldFilter() {
    if (_msgFilter != NULL) {
        delete _msgFilter;
        _msgFilter = NULL;
    }
}

bool DescFieldFilter::init(const std::string &filterDesc) {
    _msgFilter = MsgFilterCreator::createMsgFilter(filterDesc);
    if (_msgFilter == NULL) {
        return false;
    }
    return true;
}

bool DescFieldFilter::filterMsg(const common::FieldGroupReader &fieldGroupReader) {
    assert(_msgFilter);
    return _msgFilter->filterMsg(fieldGroupReader);
}

} // namespace filter
} // namespace swift
