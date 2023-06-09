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

#include "matchdoc/Reference.h"

#include "ha3/isearch.h"

namespace isearch {
namespace compound_scorer {

template<class T>
class MLRWorkerBase {
public:
    MLRWorkerBase(): _groupMLR(0) {}
    ~MLRWorkerBase() {}
    bool init(const std::string& value);
protected:
    score_t doWork(T fieldVal);
    score_t                         _groupMLR;
    std::vector<T>                  _vecMLRKey;
    std::vector<score_t>            _vecMLRValue;
};

template<class T>
bool MLRWorkerBase<T>::init(const std::string& value)
{
    _vecMLRKey.clear();
    _vecMLRValue.clear();

    const std::vector<std::string>& vecItem
        = autil::StringUtil::split(value, ITEMSEP, true);
    if (vecItem.empty()) {
        return false;
    }

    typename std::map<T, score_t> mapMLR;
    for (auto iter = vecItem.begin(); iter != vecItem.end(); ++iter) {
        const std::vector<std::string>& vecItemVal
            = autil::StringUtil::split(*iter, KVSEP, true);
        if (vecItemVal.size() == 1) {
            _groupMLR = autil::StringUtil::fromString<score_t>(vecItemVal[0]);
        } else if (vecItemVal.size() == 2) {
            _groupMLR = 0;
            T key = autil::StringUtil::fromString<T>(vecItemVal[0]);
            score_t val = autil::StringUtil::fromString<score_t>(vecItemVal[1]);
            mapMLR.insert(std::make_pair(key, val));
        } else {
            return false;
        }
    }

    //insert and sort
    for(auto iter = mapMLR.begin(); iter != mapMLR.end(); ++iter) {
        _vecMLRKey.push_back(iter->first);
        _vecMLRValue.push_back(iter->second);
    }
    return true;
}

template<class T>
score_t MLRWorkerBase<T>::doWork(T fieldVal) {
    if (_vecMLRKey.empty()) {
        return fieldVal * _groupMLR;
    }
    auto iter = std::lower_bound(_vecMLRKey.begin(), _vecMLRKey.end(), fieldVal);
    if (iter == _vecMLRKey.end() || fieldVal != *iter) {
        return 0;
    }
    return _vecMLRValue[iter-_vecMLRKey.begin()];
}

template<class T>
class MLRWorker : public MLRWorkerBase<T> {
public:
    MLRWorker(const matchdoc::Reference<T>* ref,score_t MLRWeight,
              matchdoc::Reference<isearch::common::Tracer>* traceRef)
        : _MLRWeight(MLRWeight)
        , _ref(ref)
        , _traceRefer(traceRef)
    {
    }
    bool operator()(score_t &PS, matchdoc::MatchDoc &matchDoc);
private:
    score_t                                     _MLRWeight;
    const matchdoc::Reference<T>* _ref;
    matchdoc::Reference<isearch::common::Tracer>*  _traceRefer;
};

template<class T>
bool MLRWorker<T>::operator()(score_t &PS, matchdoc::MatchDoc &matchDoc)
{
    T fieldVal = _ref->get(matchDoc);
    score_t tmpPS = this->doWork(fieldVal);
    if (this->_vecMLRKey.empty()) {
        PS += tmpPS;
    } else if (tmpPS) {
        PS = PS * _MLRWeight + tmpPS;
    }
    RANK_TRACE(DEBUG, matchDoc, "after MLRScore: %f", PS);
    return true;
}

template<class T>
class MLRWorker<autil::MultiValueType<T> > : public MLRWorkerBase<T> {
    typedef const matchdoc::Reference<autil::MultiValueType<T> > REF_T;
public:
    MLRWorker(REF_T* ref,score_t MLRWeight,
              matchdoc::Reference<isearch::common::Tracer>* traceRef)
        : _MLRWeight(MLRWeight)
        , _ref(ref)
        , _traceRefer(traceRef)
    {
    }
    bool operator()(score_t &PS, matchdoc::MatchDoc &matchDoc);
private:
    score_t _MLRWeight;
    REF_T * _ref;
    matchdoc::Reference<isearch::common::Tracer>* _traceRefer;
};

template<class T>
bool MLRWorker<autil::MultiValueType<T> >::operator()(score_t &PS,
        matchdoc::MatchDoc &matchDoc)
{
    autil::MultiValueType<T> fieldVals = _ref->get(matchDoc);
    score_t tmpPS = 0;
    for(size_t i = 0; i < fieldVals.size(); ++i) {
        tmpPS += this->doWork(fieldVals[i]);
    }
    if (this->_vecMLRKey.empty()) {
        PS += tmpPS;
    } else if(tmpPS) {
        PS = PS * _MLRWeight + tmpPS;
    }
    RANK_TRACE(DEBUG, matchDoc, "after MLRScore: %f", PS);
    return true;
}

template<class T>
class MLRWorkerSort;

template<class T>
class MLRWorkerSort<autil::MultiValueType<T> > : public MLRWorkerBase<T>
{
    typedef const matchdoc::Reference<autil::MultiValueType<T> > REF_T;
public:
    MLRWorkerSort(REF_T* ref,score_t MLRWeight,
                  matchdoc::Reference<isearch::common::Tracer>* traceRef)
        : _MLRWeight(MLRWeight)
        , _ref(ref)
        , _traceRefer(traceRef)
    {
    }
    bool operator()(score_t &PS, matchdoc::MatchDoc &matchDoc);
private:
    score_t _MLRWeight;
    REF_T *_ref;
    matchdoc::Reference<isearch::common::Tracer> *_traceRefer;
};

template<class T>
bool MLRWorkerSort<autil::MultiValueType<T> >::operator()(score_t &PS,
        matchdoc::MatchDoc &matchDoc)
{
    autil::MultiValueType<T> fieldVals = _ref->get(matchDoc);
    if (this->_vecMLRKey.empty()) {
        for(size_t i = 0; i < fieldVals.size(); ++i){
            PS += fieldVals[i] * this->_groupMLR;
        }
        RANK_TRACE(DEBUG, matchDoc, "after MLRScore: %f", PS);
        return true;
    }
    auto first1 = fieldVals.data();
    auto end1 = fieldVals.data() + fieldVals.size();
    auto first2 = this->_vecMLRKey.begin();
    auto end2 = this->_vecMLRKey.end();
    score_t tmpPS = 0;

    while (first1 != end1 && first2 != end2) {
        if (*first1 == *first2) {
            tmpPS += this->_vecMLRValue[first2 - this->_vecMLRKey.begin()];
            ++first1;
            ++first2;
        } else if (*first1 < *first2) {
            first1 = std::lower_bound(first1,end1,*first2);
        } else {
            first2 = std::lower_bound(first2,end2,*first1);
        }
    }
    if (tmpPS){
        PS = PS * _MLRWeight + tmpPS;
        RANK_TRACE(DEBUG, matchDoc, "after MLRScore: %f", PS);
    }
    return true;
}

} // namespace compound_scorer
} // namespace isearch

