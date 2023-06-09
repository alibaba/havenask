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
#pragma once

#include <stdint.h>
#include <vector>


namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

class PhaseTwoSearchInfo
{
public:
    PhaseTwoSearchInfo()
        : summaryLatency(0)
    {
    }
    ~PhaseTwoSearchInfo() {}
private:
    PhaseTwoSearchInfo(const PhaseTwoSearchInfo &);
    PhaseTwoSearchInfo& operator=(const PhaseTwoSearchInfo &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    void mergeFrom(const std::vector<PhaseTwoSearchInfo*> &inputSearchInfos);
public:
    int64_t summaryLatency;
};

} // namespace common
} // namespace isearch
