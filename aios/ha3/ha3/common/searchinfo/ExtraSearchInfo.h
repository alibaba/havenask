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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <memory>

namespace autil {
class DataBuffer;
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class PhaseTwoSearchInfo;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class ExtraSearchInfo
{
public:
    ExtraSearchInfo()
        : seekDocCount(0)
        , phaseTwoSearchInfo(NULL)
    {
    }
    ~ExtraSearchInfo() {}
private:
    ExtraSearchInfo(const ExtraSearchInfo &);
    ExtraSearchInfo& operator=(const ExtraSearchInfo &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

    void toString(std::string &str, autil::mem_pool::Pool *pool);
    void fromString(const std::string &str, autil::mem_pool::Pool *pool);
public:
    std::string otherInfoStr;
    uint32_t seekDocCount;
    PhaseTwoSearchInfo *phaseTwoSearchInfo;
};

typedef std::shared_ptr<ExtraSearchInfo> ExtraSearchInfoPtr;

} // namespace common
} // namespace isearch
