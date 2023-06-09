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

#include <stddef.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/AggregateResult.h"

namespace matchdoc {
class ReferenceBase;
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace common {

template<typename T>
class AggSingleFunResultReader {
public:
    typedef AggregateResult::AggExprValueMap::const_iterator InnerIterator;
public:
    class Iterator {
    public:
        Iterator() {}
    public:
        const Iterator *operator->() const {return this;}
        const Iterator &operator*() const {return *this;}

        Iterator operator++(int) {Iterator tmp = *this; ++_it; return tmp;}
        Iterator& operator++() {++_it; return *this;}

        bool operator==(const Iterator &rhs) {return _it == rhs._it && _refer == rhs._refer;}
        bool operator!=(const Iterator &rhs) {return !(*this == rhs);}

        const std::string &getKeyValue() const {return _it->first;}
        T getFunValue() const {return _refer->get(_it->second);}
    private:
        Iterator(InnerIterator it, const matchdoc::Reference<T> *refer)
            : _it(it), _refer(refer) {}
    private:
        InnerIterator _it;
        const matchdoc::Reference<T> *_refer;
    private:
        friend class AggSingleFunResultReader<T>;
    };
public:
    size_t size() const {return _count;}
    Iterator begin() const {return Iterator(_begin, _refer);}
    Iterator end() const {return Iterator(_end, _refer);}
private:
    uint32_t _count;
    InnerIterator _begin;
    InnerIterator _end;
    const matchdoc::Reference<T> *_refer;
private:
    friend class AggResultReader;
};

class AggResultReader
{
public:
    AggResultReader(const AggregateResultsPtr &aggResults);
    ~AggResultReader();
public:
    template<typename T>
    bool getAggResult(const std::string &key, const std::string &funDes,
                      const std::string &keyValue, T &funValue);

    template<typename T>
    bool getAggSingleFunResultReader(const std::string &key,
            const std::string &funDes,
            AggSingleFunResultReader<T> &sigleRunResult);
private:
    typedef std::pair<const AggregateResult::AggExprValueMap *,
                      const matchdoc::ReferenceBase *> BasePair;

    BasePair getAggResult(const std::string &key, const std::string &funDes);

    template<typename T>
    struct TypedPair : public std::pair<const AggregateResult::AggExprValueMap *,
                                        const matchdoc::Reference<T> *>
    {};

    template<typename T>
    bool getAggResult(const std::string &key, const std::string &funDes,
                      TypedPair<T> &typedPair);
private:
    std::vector<bool> _constructedMap;
    AggregateResultsPtr _aggResults;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AggResultReader> AggResultReaderPtr;

template<typename T>
bool AggResultReader::getAggResult(const std::string &key, const std::string &funDes,
                                   const std::string &keyValue, T &funValue)
{
    TypedPair<T> typedPair;
    if (!getAggResult(key, funDes, typedPair)) {
        return false;
    }

    AggregateResult::AggExprValueMap::const_iterator it = (typedPair.first)->find(keyValue);
    if (it == (typedPair.first)->end()) {
        return false;
    }
    funValue = typedPair.second->get(it->second);
    return true;
}

template<typename T>
bool AggResultReader::getAggSingleFunResultReader(
        const std::string &key, const std::string &funDes,
        AggSingleFunResultReader<T> &singleFunResult)
{
    TypedPair<T> typedPair;
    if (!getAggResult(key, funDes, typedPair)) {
        return false;
    }

    singleFunResult._count = typedPair.first->size();

    singleFunResult._begin = typedPair.first->begin();
    singleFunResult._end = typedPair.first->end();

    singleFunResult._refer = typedPair.second;

    return true;
}

template <typename T>
bool AggResultReader::getAggResult(const std::string &key, const std::string &funDes,
                                   TypedPair<T> &typedPair)
{
    BasePair p = getAggResult(key, funDes);
    if (!p.first || !p.second) {
        return false;
    }

    auto refer = dynamic_cast<const matchdoc::Reference<T> *>(p.second);
    if (!refer) {
        return false;
    }

    typedPair.first = p.first;
    typedPair.second = refer;

    return true;
}

} // namespace common
} // namespace isearch

