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
#include "suez/turing/expression/util/SummaryInfo.h"

#include <cstddef>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"

using namespace std;
using namespace autil::legacy;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SummaryInfo);

SummaryInfo::SummaryInfo() {}

SummaryInfo::~SummaryInfo() {}

size_t SummaryInfo::getFieldCount() const { return _fieldNameVector.size(); }

string SummaryInfo::getFieldName(uint32_t pos) const {
    if (pos >= _fieldNameVector.size() || pos < 0) {
        AUTIL_LOG(WARN, "Not exist position. Size[%zd], pos[%d]", _fieldNameVector.size(), pos);
        return "";
    }
    return _fieldNameVector[pos];
}

void SummaryInfo::addFieldName(const string &fieldName) {
    Name2PosMap::const_iterator iter = _name2PosMap.find(fieldName);
    if (iter != _name2PosMap.end()) {
        AUTIL_LOG(WARN, "Duplicate summary field: %s", fieldName.c_str());
        return;
    }
    _name2PosMap[fieldName] = _fieldNameVector.size();
    _fieldNameVector.push_back(fieldName);
}

void SummaryInfo::stealSummaryInfos(StringVector &summaryInfos) {
    summaryInfos = _fieldNameVector;
    _fieldNameVector.clear();
    _name2PosMap.clear();
}

bool SummaryInfo::exist(const string &fieldName) const { return _name2PosMap.find(fieldName) != _name2PosMap.end(); }

void SummaryInfo::Jsonize(JsonWrapper &json) {
    json.Jsonize("fieldNames", _fieldNameVector);
    if (json.GetMode() == FROM_JSON) {
        for (size_t i = 0; i < _fieldNameVector.size(); i++) {
            _name2PosMap[_fieldNameVector[i]] = i;
        }
    }
}

} // namespace turing
} // namespace suez
