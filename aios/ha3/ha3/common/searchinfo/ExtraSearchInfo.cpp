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
#include "ha3/common/searchinfo/ExtraSearchInfo.h"

#include <iosfwd>

#include "autil/DataBuffer.h"

#include "ha3/common/searchinfo/PhaseTwoSearchInfo.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;

namespace isearch {
namespace common {

void ExtraSearchInfo::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(otherInfoStr);
    dataBuffer.write(seekDocCount);
    dataBuffer.write(phaseTwoSearchInfo);
}

void ExtraSearchInfo::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(otherInfoStr);
    dataBuffer.read(seekDocCount);
    dataBuffer.read(phaseTwoSearchInfo);
}

void ExtraSearchInfo::toString(string &str, autil::mem_pool::Pool *pool) {
    autil::DataBuffer dataBuffer(1024, pool);
    serialize(dataBuffer);
    str.assign(dataBuffer.getData(), dataBuffer.getDataLen());
}

void ExtraSearchInfo::fromString(const std::string &str,
                                 autil::mem_pool::Pool *pool)
{
    autil::DataBuffer dataBuffer((void *)str.c_str(), str.size(), pool);
    deserialize(dataBuffer);
}

} // namespace common
} // namespace isearch
