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
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/Hit.h"
#include "ha3/common/Request.h"
#include "ha3/common/SortExprMeta.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "indexlib/indexlib.h"
#include "matchdoc/Reference.h"

namespace isearch {
namespace common {
class ErrorResult;
class Hits;
class MatchDocs;
struct ExtraDocIdentifier;
}  // namespace common
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc

namespace isearch {
namespace qrs {

class MatchDocs2Hits
{
public:
    MatchDocs2Hits();
    ~MatchDocs2Hits();
private:
    MatchDocs2Hits(const MatchDocs2Hits &);
    MatchDocs2Hits& operator=(const MatchDocs2Hits &);
public:
    common::Hits* convert(const common::MatchDocs *matchDocs, 
                          const common::RequestPtr &requestPtr,
                          const std::vector<common::SortExprMeta>& sortExprMetaVec,
                          common::ErrorResult& errResult,
                          const std::vector<std::string> &clusterList);
private:
    common::HitPtr convertOne(const matchdoc::MatchDoc matchDoc);
    void fillExtraDocIdentifier(const matchdoc::MatchDoc matchDoc,
                                common::ExtraDocIdentifier &extraDocIdentifier);
    void clear();
    void extractNames(const matchdoc::ReferenceVector &referVec, 
                      const std::string &prefix,
                      std::vector<std::string> &nameVec) const;
    bool initAttributeReferences(const common::RequestPtr &requestPtr,
                                 const common::Ha3MatchDocAllocatorPtr &allocatorPtr);
    void initUserData(const common::Ha3MatchDocAllocatorPtr &allocatorPtr);
    void initIdentifiers(const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                         const common::MatchDocs *matchDocs);
private:
    common::Hits *_hits;
    bool _hasPrimaryKey;
    std::vector<const matchdoc::ReferenceBase *> _sortReferVec;
    matchdoc::ReferenceVector _userDataReferVec;
    std::vector<std::string> _userDataNames;
    std::vector<std::pair<std::string, matchdoc::ReferenceBase *>> _attributes;
    const matchdoc::Reference<hashid_t> *_hashIdRef;
    const matchdoc::Reference<clusterid_t> *_clusterIdRef;
    matchdoc::Reference<common::Tracer> *_tracerRef;
    const matchdoc::Reference<uint64_t> *_primaryKey64Ref;
    const matchdoc::Reference<primarykey_t> *_primaryKey128Ref;
    const matchdoc::Reference<FullIndexVersion> *_fullVersionRef;
    const matchdoc::Reference<versionid_t> *_indexVersionRef;
    const matchdoc::Reference<uint32_t> *_ipRef;
    const matchdoc::ReferenceBase *_rawPkRef;
private:
    friend class MatchDocs2HitsTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MatchDocs2Hits> MatchDocs2HitsPtr;

} // namespace qrs
} // namespace isearch

