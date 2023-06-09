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

#include <vector>

#include "ha3/common/ErrorDefine.h"
#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
namespace mem_pool {
template <typename T> class PoolVector;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class Ha3MatchDocAllocator;
class MatchDocs;
}  // namespace common
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
class ReferenceBase;
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace proxy {

class MatchDocDeduper
{
public:
    MatchDocDeduper(common::Ha3MatchDocAllocator *allocator,
                    bool needDedup, bool allocatorAppendOnly = false);
    ~MatchDocDeduper();
private:
    MatchDocDeduper(const MatchDocDeduper &);
    MatchDocDeduper& operator=(const MatchDocDeduper &);
public:
    ErrorCode dedup(const std::vector<common::MatchDocs *> &toDedup,
               autil::mem_pool::PoolVector<matchdoc::MatchDoc> &resultMatchDocs);
private:
    void initReference();
    ErrorCode noDedupMeger(const std::vector<common::MatchDocs *> &toDedup,
                      autil::mem_pool::PoolVector<matchdoc::MatchDoc> &resultMatchDocs);
    template <typename PKType>
    ErrorCode typedDedup(const std::vector<common::MatchDocs *> &toDedup,
                         autil::mem_pool::PoolVector<matchdoc::MatchDoc> &resultMatchDocs);
    bool canDedup(const std::vector<common::MatchDocs *> &toDedup);
    bool mergeMatchDocAllocator(common::MatchDocs *matchDocs,
                                std::vector<matchdoc::MatchDoc> &matchDocVect);
private:
    common::Ha3MatchDocAllocator *_allocator;
    bool _needDedup;
    matchdoc::Reference<clusterid_t> *_clusterIdRef;
    matchdoc::Reference<FullIndexVersion> *_fullVersionRef;
    matchdoc::ReferenceBase *_primaryKeyRef;
    bool _allocatorAppendOnly;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace proxy
} // namespace isearch

