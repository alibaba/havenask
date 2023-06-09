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
#include "ha3/common/RankHint.h"

#include <iosfwd>

#include "autil/DataBuffer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"


using namespace std;
using namespace autil;
namespace isearch {
namespace common {

RankHint::RankHint() {
    sortPattern = indexlibv2::config::sp_asc;
}

void RankHint::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(sortField);
    dataBuffer.write(sortPattern);
}

void RankHint::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(sortField);
    dataBuffer.read(sortPattern);
}

string RankHint::toString() const {
    string rankHintStr;
    rankHintStr.append("sortfield:");
    rankHintStr.append(sortField);
    rankHintStr.append(",sortpattern:");
    rankHintStr.append(StringUtil::toString((int)sortPattern));
    return rankHintStr;
}

} // namespace common
} // namespace isearch
