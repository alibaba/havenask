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
#include <memory>
#include <string>

#include "matchdoc/MatchDocAllocator.h"

#include "ha3/common/AttributeItem.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class ReferenceIdManager;
}  // namespace common
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
class ReferenceBase;
}  // namespace matchdoc

namespace isearch {
namespace common {

class Ha3MatchDocAllocator : public matchdoc::MatchDocAllocator
{
public:
    Ha3MatchDocAllocator(autil::mem_pool::Pool *pool, bool useSubDoc = false);
    ~Ha3MatchDocAllocator();
public:
    void initPhaseOneInfoReferences(uint8_t phaseOneInfoFlag);
    uint32_t requireReferenceId(const std::string &refName);
public:
    static common::AttributeItemPtr createAttributeItem(
            matchdoc::ReferenceBase *ref,
            matchdoc::MatchDoc matchDoc);
private:
    void registerTypes();
private:
    ReferenceIdManager *_refIdManager;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Ha3MatchDocAllocator> Ha3MatchDocAllocatorPtr;

} // namespace common
} // namespace isearch

