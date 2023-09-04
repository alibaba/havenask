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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace matchdoc {

class MatchDocAllocator;

typedef std::shared_ptr<MatchDocAllocator> MatchDocAllocatorPtr;
class ReferenceBase;

// TODO: use memory-chunk copy to optimize join performance
class MatchDocJoiner {
public:
    MatchDocJoiner() : _hasExtend(false) {}
    ~MatchDocJoiner() {}

private:
    MatchDocJoiner(const MatchDocJoiner &);
    MatchDocJoiner &operator=(const MatchDocJoiner &);

public:
    typedef std::map<std::string, std::string> AliasMap;

public:
    bool init(const MatchDocAllocator *leftAllocator,
              const MatchDocAllocator *rightAllocator,
              autil::mem_pool::Pool *pool,
              const AliasMap &leftMap = AliasMap(),
              const AliasMap &rightMap = AliasMap());

    const MatchDocAllocatorPtr &getJoinMatchDocAllocator() const;

    MatchDoc join(const MatchDoc &leftDoc, const MatchDoc &rightDoc);
    // typedef std::vector<MatchDoc> MatchDocVector;
    // void join(const MatchDocVector& leftDocs, const MatchDocVector& rightDocs,
    //           MatchDocVector& joinedDocs);

private:
    typedef std::pair<const ReferenceBase *, const ReferenceBase *> ReferencePair;
    typedef std::vector<ReferencePair> ReferencePairVector;

private:
    bool copyReference(const MatchDocAllocator *srcAllocator,
                       autil::mem_pool::Pool *pool,
                       ReferencePairVector &referenceVec,
                       const AliasMap &aliasMap);

    void
    joinFromMatchDoc(const ReferencePairVector &referenceVec, const MatchDoc &srcMatchDoc, const MatchDoc &dstMatchDoc);

    const std::string &aliasName(const std::string &name, const AliasMap &aliasMap) const;

private:
    MatchDocAllocatorPtr _joinAllocator;
    ReferencePairVector _leftReferenceVec;
    ReferencePairVector _rightReferenceVec;
    bool _hasExtend;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<MatchDocJoiner> MatchDocJoinerPtr;
} // namespace matchdoc
