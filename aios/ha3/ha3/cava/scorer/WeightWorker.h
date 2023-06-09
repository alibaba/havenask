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

#include "matchdoc/Reference.h"

#include "ha3/isearch.h"

namespace isearch {
namespace compound_scorer {

template<class T>
class WeightWorker {
public:
    WeightWorker(const matchdoc::Reference<T>* ref,bool ASC,
                 int64_t factor, matchdoc::Reference<isearch::common::Tracer>* traceRef)
        :_ref(ref),_bASC(ASC),_factor(factor),_traceRefer(traceRef)
    {
    }
    ~WeightWorker() {}
    bool init(const std::string& value);
    bool operator()(score_t &PS, matchdoc::MatchDoc &matchDoc);
protected:
    score_t doWork(T fieldVal);
private:
    const matchdoc::Reference<T>*     _ref;
    bool                            _bASC;
    score_t                         _groupWeight;
    int64_t                         _factor;
    std::map<T, score_t>            _mapWeight;
    matchdoc::Reference<isearch::common::Tracer>* _traceRefer;
};

template<class T>
bool WeightWorker<T>::init(const std::string& value)
{
    _mapWeight.clear();
    const std::vector<std::string> &vecItem = autil::StringUtil::split(value, ITEMSEP, true);
    if (vecItem.empty()) {
        return false;
    }
    for (auto iter = vecItem.begin(); iter != vecItem.end(); ++iter) {
        const std::vector<std::string> &vecItemVal
            = autil::StringUtil::split(*iter, KVSEP, true);
        if (vecItemVal.size() == 1) {
            _groupWeight = autil::StringUtil::fromString<score_t>(vecItemVal[0]);
        } else if(vecItemVal.size() == 2) {
            _groupWeight = 0;
            T key = autil::StringUtil::fromString<T>(vecItemVal[0]);
            score_t val = autil::StringUtil::fromString<score_t>(vecItemVal[1]);
            _mapWeight.insert(std::make_pair(key,val));
        } else {
            return false;
        }
    }
    return true;
}

template<class T>
bool WeightWorker<T>::operator()(score_t &PS, matchdoc::MatchDoc &matchDoc)
{
    T fieldVal = _ref->get(matchDoc);
    PS += doWork(fieldVal);
    RANK_TRACE(DEBUG, matchDoc, "after WeightScore: %f", PS);
    return true;
}

template<class T>
score_t WeightWorker<T>::doWork(T fieldVal)
{
    if (_mapWeight.empty()) {
        return fieldVal * _factor * (_bASC ? (100 - _groupWeight) : _groupWeight);
    } else {
        score_t weight = 0;
        auto iter = _mapWeight.find(fieldVal);
        if (iter != _mapWeight.end()) {
            weight = iter->second;
        }
        return _factor * (_bASC ? (100 - weight) : weight);
    }
    return 0;
}

} // namespace compound_scorer
} // namespace isearch


