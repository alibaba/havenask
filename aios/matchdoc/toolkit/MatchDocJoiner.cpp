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
#include "matchdoc/toolkit/MatchDocJoiner.h"

#include <assert.h>
#include <stddef.h>
#include <type_traits>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "matchdoc/FieldGroup.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace autil::mem_pool;

namespace matchdoc {

AUTIL_LOG_SETUP(matchdoc, MatchDocJoiner);

bool MatchDocJoiner::init(const MatchDocAllocator *leftAllocator,
                          const MatchDocAllocator *rightAllocator,
                          Pool *pool,
                          const AliasMap &leftMap,
                          const AliasMap &rightMap) {
    if (!leftAllocator || !rightAllocator || !pool) {
        return false;
    }

    if (leftAllocator->getSubDocAccessor() || rightAllocator->getSubDocAccessor()) {
        AUTIL_LOG(ERROR, "dose not support join subtables");
        return false;
    }

    _joinAllocator.reset(new MatchDocAllocator(pool));

    if (!copyReference(leftAllocator, pool, _leftReferenceVec, leftMap)) {
        return false;
    }

    if (!copyReference(rightAllocator, pool, _rightReferenceVec, rightMap)) {
        return false;
    }

    _hasExtend = false;
    return true;
}

const string &MatchDocJoiner::aliasName(const string &name, const AliasMap &aliasMap) const {
    AliasMap::const_iterator it = aliasMap.find(name);
    if (it != aliasMap.end()) {
        return it->second;
    }
    return name;
}

bool MatchDocJoiner::copyReference(const MatchDocAllocator *srcAllocator,
                                   Pool *pool,
                                   ReferencePairVector &referenceVec,
                                   const AliasMap &aliasMap) {
    assert(srcAllocator);
    referenceVec.clear();
    // iterate all references
    typedef MatchDocAllocator::FieldGroups FieldGroups;
    const FieldGroups &fieldGroups = srcAllocator->getFieldGroups();
    for (const auto &group : fieldGroups) {
        const ReferenceMap &refMap = group.second->getReferenceMap();
        // TODO: use sorted Reference by offset to optimize performance
        for (const auto &ref : refMap) {
            const string &dstName = aliasName(ref.first, aliasMap);
            ReferenceBase *srcRef = ref.second;
            ReferenceBase *dstRef = _joinAllocator->cloneReference(srcRef, dstName);
            if (!dstRef) {
                return false;
            }
            referenceVec.push_back(make_pair(srcRef, dstRef));
        }
    }
    return true;
}

const MatchDocAllocatorPtr &MatchDocJoiner::getJoinMatchDocAllocator() const { return _joinAllocator; }

MatchDoc MatchDocJoiner::join(const MatchDoc &leftDoc, const MatchDoc &rightDoc) {
    if (unlikely(!_hasExtend)) {
        _joinAllocator->extend();
        _hasExtend = true;
    }

    MatchDoc joinedDoc = _joinAllocator->allocate();
    joinFromMatchDoc(_leftReferenceVec, leftDoc, joinedDoc);
    joinFromMatchDoc(_rightReferenceVec, rightDoc, joinedDoc);
    return joinedDoc;
}

void MatchDocJoiner::joinFromMatchDoc(const ReferencePairVector &referenceVec,
                                      const MatchDoc &srcMatchDoc,
                                      const MatchDoc &dstMatchDoc) {
    for (size_t i = 0; i < referenceVec.size(); ++i) {
        const ReferenceBase *srcRef = referenceVec[i].first;
        const ReferenceBase *dstRef = referenceVec[i].second;
        dstRef->cloneConstruct(dstMatchDoc, srcMatchDoc, srcRef);
    }
}
} // namespace matchdoc
