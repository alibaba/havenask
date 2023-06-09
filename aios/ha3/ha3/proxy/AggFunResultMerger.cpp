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
#include "ha3/proxy/AggFunResultMerger.h"

#include <assert.h>
#include <cstddef>
#include <vector>

#include "alog/Logger.h"
#include "autil/Hyperloglog.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

#include "autil/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace proxy {
AUTIL_LOG_SETUP(ha3, AggFunResultMerger);

AggFunResultMerger::AggFunResultMerger() { 
}

AggFunResultMerger::~AggFunResultMerger() { 
    for (vector<AggFunMergerBase*>::iterator it = _aggFunMergers.begin();
         it != _aggFunMergers.end(); ++it) 
    {
        delete *it;
    }
    _aggFunMergers.clear();
}

void AggFunResultMerger::merge(matchdoc::MatchDoc dstDoc, const matchdoc::MatchDoc srcDoc, autil::mem_pool::Pool* pool) {
    for (vector<AggFunMergerBase*>::iterator it = _aggFunMergers.begin();
         it != _aggFunMergers.end(); ++it)
    {
        (*it)->merge(dstDoc, srcDoc, pool);
    }
}

void AggFunResultMerger::addAggFunMerger(AggFunMergerBase *aggFunMerger) {
    if (aggFunMerger) {
        _aggFunMergers.push_back(aggFunMerger);
    }
}

template<template<class K> class TypeAggFunMerger>
static AggFunMergerBase *doCreateAggFunMerger(matchdoc::ReferenceBase *refer) {
    auto valueType = refer->getValueType();
    assert(!valueType.isMultiValue());
#define CREATE_AGG_FUN_MERGER_HELPER(vt_type)                           \
    case vt_type:                                                       \
    {                                                                   \
        typedef VariableTypeTraits<vt_type, false>::AttrItemType T;     \
        matchdoc::Reference<T> *vr                                      \
            = dynamic_cast<matchdoc::Reference<T> *>(refer);            \
        assert(vr);                                                     \
        return new TypeAggFunMerger<T>(vr);                             \
    }                                                                   \
    break                                                               \

    auto vt = valueType.getBuiltinType();
    switch(vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(CREATE_AGG_FUN_MERGER_HELPER);
    default:
        assert(false);
        break;
    }
    return NULL;
}

AggFunMergerBase *AggFunMergerBase::createAggFunMerger(
        const std::string& funName, matchdoc::ReferenceBase *refer)
{
    if ("max" == funName) {
        return doCreateAggFunMerger<MaxAggFunMerger>(refer);
    } else if ("min" == funName) {
        return doCreateAggFunMerger<MinAggFunMerger>(refer);
    } else if ("sum" == funName) {
        return doCreateAggFunMerger<SumAggFunMerger>(refer);
    } else if ("count" == funName) {
        return doCreateAggFunMerger<CountAggFunMerger>(refer);
    } else if ("distinct_count" == funName) {
        matchdoc::Reference<autil::HllCtx> *vr =
            dynamic_cast<matchdoc::Reference<autil::HllCtx> *>(refer);
        if (vr == NULL) {
            return NULL;
        }
        return new DistinctCountAggFunMerger(vr);
    } else {
        assert(false);
        return NULL;
    }
}

AUTIL_LOG_SETUP(ha3, DistinctCountAggFunMerger);
inline void DistinctCountAggFunMerger::merge(matchdoc::MatchDoc dstDoc,
                                             const matchdoc::MatchDoc srcDoc,
                                             autil::mem_pool::Pool *pool) {
    autil::HllCtx& hllCtxSrc = _refer->getReference(srcDoc);
    autil::HllCtx& hllCtxDst = _refer->getReference(dstDoc);
    int res = autil::Hyperloglog::hllCtxMerge(&hllCtxDst, &hllCtxSrc, pool);
    if (res < 0) {
        AUTIL_LOG(ERROR, "hyperloglog merge failed");
    }
    return;
}


} // namespace proxy
} // namespace isearch

