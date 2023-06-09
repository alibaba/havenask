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
#include "table/ColumnData.h"

using namespace std;

namespace table {
std::string ColumnDataBase::toString(size_t rowIndex) const {
    std::string str = _ref->toString((*_rows)[rowIndex]);
    if (_ref->getValueType().isMultiValue()) {
        autil::StringUtil::replaceAll(str, std::string(1, autil::MULTI_VALUE_DELIMITER), "^]");
    }
    return str;
}

std::string ColumnDataBase::toOriginString(size_t rowIndex) const {
    return _ref->toString((*_rows)[rowIndex]);
}

}
