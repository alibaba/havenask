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
#include "ha3/proxy/MatchDocDeduper.h"

#include <assert.h>
#include <stdint.h>
#include <cstddef>
#include <map>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/LongHashValue.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/isearch.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;

using namespace isearch::common;
namespace isearch {
namespace proxy {

AUTIL_LOG_SETUP(ha3, MatchDocDeduper);
MatchDocDeduper::MatchDocDeduper(common::Ha3MatchDocAllocator *allocator,
                                 bool needDedup, bool allocatorAppendOnly)
    : _allocator(allocator)
    , _needDedup(needDedup)
    , _allocatorAppendOnly(allocatorAppendOnly)
{
    initReference();
}

MatchDocDeduper::~MatchDocDeduper() { 
}

ErrorCode MatchDocDeduper::dedup(const vector<common::MatchDocs *> &toDedup,
                            PoolVector<matchdoc::MatchDoc> &resultMatchDocs)
{
    if (!canDedup(toDedup)) {
        return noDedupMeger(toDedup, resultMatchDocs);
    }
    assert(_primaryKeyRef);
    auto valueType = _primaryKeyRef->getValueType();
    auto pkType = valueType.getBuiltinType();
    bool isMulti = valueType.isMultiValue();
    ErrorCode ec = ERROR_NONE;
#define SWICH_PRIMARYKEY_TYPE_HELPER(vt)                                \
    case vt:                                                            \
    {                                                                   \
        if (isMulti) {                                                  \
            typedef VariableTypeTraits<vt, true>::AttrExprType T;       \
            ec = typedDedup<T>(toDedup, resultMatchDocs);               \
        } else {                                                        \
            typedef VariableTypeTraits<vt, false>::AttrExprType T;      \
            ec =typedDedup<T>(toDedup, resultMatchDocs);                \
        }                                                               \
    }                                                                   \
    break
    
    switch(pkType) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(SWICH_PRIMARYKEY_TYPE_HELPER);
        SWICH_PRIMARYKEY_TYPE_HELPER(vt_string);
    case vt_hash_128:
    {
        ec = typedDedup<primarykey_t>(toDedup, resultMatchDocs);
        break;
    }
    default:
        break;
    }
    return ec;
}

bool MatchDocDeduper::canDedup(const vector<common::MatchDocs *> &toDedup) {
    return NULL != _primaryKeyRef && toDedup.size() > 1 && _needDedup;
}

ErrorCode MatchDocDeduper::noDedupMeger(const vector<common::MatchDocs *> &toDedup,
                                   PoolVector<matchdoc::MatchDoc> &resultMatchDocs)
{
    ErrorCode ec = ERROR_NONE;
    for (auto matchDocs : toDedup) {
        vector<matchdoc::MatchDoc> matchDocVect;
        if (!mergeMatchDocAllocator(matchDocs, matchDocVect)) {
            ec = ERROR_VSA_NOT_SAME;
            AUTIL_LOG(WARN, "allocator can't merge, drop MatchDocs.");
            continue;
        }
        for (auto doc : matchDocVect) {
            resultMatchDocs.push_back(doc);
        }
    }
    return ec;
}

template <typename PKType>
ErrorCode MatchDocDeduper::typedDedup(const vector<common::MatchDocs *> &toDedup,
                                 PoolVector<matchdoc::MatchDoc> &resultMatchDocs)
{
    auto pkRef = dynamic_cast<matchdoc::Reference<PKType> *>(_primaryKeyRef);
    assert(pkRef != NULL);
    // searcher return has primary key, must has extra doc identifier.
    map<PKType, matchdoc::MatchDoc> dedupMap;
    ErrorCode ec = ERROR_NONE;
    for (auto matchDocs : toDedup) {
        vector<matchdoc::MatchDoc> matchDocVect;
        if (!mergeMatchDocAllocator(matchDocs, matchDocVect)) {
            ec = ERROR_VSA_NOT_SAME;
            AUTIL_LOG(WARN, "allocator can't merge, drop MatchDocs.");
            continue;
        }
        for (uint32_t j = 0; j < matchDocVect.size(); ++j) {
            auto matchDoc = matchDocVect[j];
            PKType pk  = pkRef->getReference(matchDoc);
            typename map<PKType, matchdoc::MatchDoc>::iterator it = dedupMap.find(pk);
            if (it == dedupMap.end()) {
                dedupMap.insert(std::make_pair(pk, matchDoc));
                continue;
            }
            auto delMatchDoc = matchDoc;
            auto oldClusterId = (clusterid_t)0;
            auto newClusterId = (clusterid_t)0;
            if (_clusterIdRef) {
                 oldClusterId = _clusterIdRef->get(it->second);
                 newClusterId = _clusterIdRef->get(matchDoc);
            }
            //keep first cluster highest full index version
            //Free old version match doc
            if (oldClusterId == newClusterId && _fullVersionRef) {
                auto fullVersion = _fullVersionRef->getReference(matchDoc);
                auto otherFullVersion = _fullVersionRef->getReference(it->second);
                if (otherFullVersion < fullVersion) {
                    delMatchDoc = it->second;
                    it->second = matchDoc;
                }
            }
            _allocator->deallocate(delMatchDoc);
        }
    }
    for (auto it = dedupMap.begin(); it != dedupMap.end(); ++it) {
        resultMatchDocs.push_back(it->second);
    }
    return ec;
}

bool MatchDocDeduper::mergeMatchDocAllocator(
        common::MatchDocs *matchDocs,
        vector<matchdoc::MatchDoc> &matchDocVect)
{
    const auto &newAllocator = matchDocs->getMatchDocAllocator();
    matchDocs->stealMatchDocs(matchDocVect);
    if (_allocator != newAllocator.get()) {
        vector<matchdoc::MatchDoc> newMatchDocVect;
        if (_allocatorAppendOnly) {
            if(!_allocator->mergeAllocatorOnlySame(newAllocator.get(),
                            matchDocVect, newMatchDocVect))
            {
                return false;
            }
        } else {
            if (!_allocator->mergeAllocator(newAllocator.get(),
                            matchDocVect, newMatchDocVect))
            {
                return false;
            }
        }
        swap(newMatchDocVect, matchDocVect);
    }
    return true;
}

void MatchDocDeduper::initReference() {
    _clusterIdRef = _allocator->findReference<clusterid_t>(CLUSTER_ID_REF);
    _fullVersionRef = _allocator->findReference<FullIndexVersion>(common::FULLVERSION_REF);
    _primaryKeyRef = _allocator->findReferenceWithoutType(PRIMARYKEY_REF);
    if (!_primaryKeyRef) {
        _primaryKeyRef = _allocator->findReferenceWithoutType(RAW_PRIMARYKEY_REF);
    }
}

} // namespace proxy
} // namespace isearch

