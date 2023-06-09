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

#include <string>
#include <memory>
#include <vector>

#include "matchdoc/MatchDoc.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class HllCtx;
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class ReferenceBase;
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace proxy {

class AggFunMergerBase
{
public:
    virtual ~AggFunMergerBase() {};
public:
    virtual void merge(matchdoc::MatchDoc dstDoc,
                       const matchdoc::MatchDoc srcDoc,
                       autil::mem_pool::Pool* pool) {
        merge(dstDoc, srcDoc);
    }
    
    static AggFunMergerBase *createAggFunMerger(
            const std::string &funName,
            matchdoc::ReferenceBase *refer);
private:
    virtual void merge(matchdoc::MatchDoc dstDoc,
                       const matchdoc::MatchDoc srcDoc) {}

private:
    AUTIL_LOG_DECLARE();
};

class AggFunResultMerger
{
public:
    AggFunResultMerger();
    ~AggFunResultMerger();
public:
    void addAggFunMerger(AggFunMergerBase *aggFunMerger);
    void merge(matchdoc::MatchDoc dstDoc, const matchdoc::MatchDoc srcDoc, autil::mem_pool::Pool* pool);
    std::vector<AggFunMergerBase*> _aggFunMergers;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggFunResultMerger> AggFunResultMergerPtr;



template<typename T>
class MaxAggFunMerger : public AggFunMergerBase
{
public:
    MaxAggFunMerger(matchdoc::Reference<T> *refer) {
        _refer = refer;
    }
    virtual ~MaxAggFunMerger() {};
private:
    virtual void merge(matchdoc::MatchDoc dstDoc,
                       const matchdoc::MatchDoc srcDoc);
private:
    matchdoc::Reference<T> *_refer;
private:
    AUTIL_LOG_DECLARE();
};

template<typename T>
class MinAggFunMerger : public AggFunMergerBase
{
public:
    MinAggFunMerger(matchdoc::Reference<T> *refer) {
        _refer = refer;
    }
    virtual ~MinAggFunMerger() {};
private:
    virtual void merge(matchdoc::MatchDoc dstDoc,
                       const matchdoc::MatchDoc srcDoc);
private:
    matchdoc::Reference<T> *_refer;
private:
    AUTIL_LOG_DECLARE();
};

template<typename T>
class CountAggFunMerger : public AggFunMergerBase
{
public:
    CountAggFunMerger(matchdoc::Reference<T> *refer) {
        _refer = refer;
    }
    virtual ~CountAggFunMerger() {};
public:
    virtual void merge(matchdoc::MatchDoc dstDoc,
                       const matchdoc::MatchDoc srcDoc);
private:
    matchdoc::Reference<T> *_refer;
private:
    AUTIL_LOG_DECLARE();
};

template<typename T>
class SumAggFunMerger : public AggFunMergerBase
{
public:
    SumAggFunMerger(matchdoc::Reference<T> *refer) {
        _refer = refer;
    }
    virtual ~SumAggFunMerger() {};
private:
    virtual void merge(matchdoc::MatchDoc dstDoc,
                       const matchdoc::MatchDoc srcDoc);
private:
    matchdoc::Reference<T> *_refer;
private:
    AUTIL_LOG_DECLARE();
};

class DistinctCountAggFunMerger : public AggFunMergerBase
{
public:
    DistinctCountAggFunMerger(matchdoc::Reference<autil::HllCtx> *refer) {
        _refer = refer;
    }
    virtual ~DistinctCountAggFunMerger() {};
public:
    virtual void merge(matchdoc::MatchDoc dstDoc,
                       const matchdoc::MatchDoc srcDoc,
                       autil::mem_pool::Pool* pool);
private:
    matchdoc::Reference<autil::HllCtx> *_refer;
private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////
template<typename T>
inline void MaxAggFunMerger<T>::merge(matchdoc::MatchDoc dstDoc,
                               const matchdoc::MatchDoc srcDoc)
{
    T value1 = _refer->get(srcDoc);
    T value2 = _refer->get(dstDoc);
    _refer->set(dstDoc, value1 > value2 ? value1 : value2);
}

template<typename T>
inline void MinAggFunMerger<T>::merge(matchdoc::MatchDoc dstDoc,
                               const matchdoc::MatchDoc srcDoc)
{
    T value1 = _refer->get(srcDoc);
    T value2 = _refer->get(dstDoc);
    _refer->set(dstDoc, value1 < value2 ? value1 : value2);
}

template<typename T>
inline void CountAggFunMerger<T>::merge(matchdoc::MatchDoc dstDoc,
                               const matchdoc::MatchDoc srcDoc)
{
    T countValue1 = _refer->get(srcDoc);
    T countValue2 = _refer->get(dstDoc);
    _refer->set(dstDoc, countValue1 + countValue2);
}

template<typename T>
inline void SumAggFunMerger<T>::merge(matchdoc::MatchDoc dstDoc,
                               const matchdoc::MatchDoc srcDoc)
{
    T sumValue1 = _refer->get(srcDoc);
    T sumValue2 = _refer->get(dstDoc);
    _refer->set(dstDoc, sumValue1 + sumValue2);
}




} // namespace proxy
} // namespace isearch

