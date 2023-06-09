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
#ifndef ISEARCH_SUBDOCACCESSOR_H
#define ISEARCH_SUBDOCACCESSOR_H

#include "matchdoc/Reference.h"
#include "matchdoc/MatchDocAllocator.h"

namespace matchdoc {

class SubDocAccessor {
public:
    SubDocAccessor(MatchDocAllocator *subAllocator,
                   const Reference<MatchDoc> *first,
                   Reference<MatchDoc> *current,
                   const Reference<MatchDoc> *next)
        : _first(first)
        , _current(current)
        , _next(next)
    {
    }
public:
    template<typename SubProcessor>
    void foreach(MatchDoc doc, SubProcessor &processor) const;

    size_t getSubDocIds(MatchDoc doc, std::vector<int32_t> &subDocids) const;
    void setSubDocIds(MatchDoc doc, const std::vector<int32_t> &subDocids,
                      size_t &beginPos);

    inline size_t getSubDocs(MatchDoc doc, std::vector<MatchDoc> &subDocs) const;
    inline void setSubDocs(MatchDoc doc, const std::vector<MatchDoc> &subDocs,
                           size_t &beginPos);
    template<typename Processor>
    void foreachFlatten(MatchDoc parentDoc, MatchDocAllocator *allocator,
                        Processor &processor) const;

    bool allSubDocSetDeletedFlag (MatchDoc doc) const {
        MatchDoc &current = _current->getReference(doc);
        const Reference<MatchDoc> *nextRef = _next;
        MatchDoc originCurrent = current;
        MatchDoc next = _first->get(doc);
        while (next != INVALID_MATCHDOC) {
          current = next;
          next = nextRef->get(current);
          if (!current.isDeleted())
              return false;
        }
        current = originCurrent;
        return true;
    }

    bool allSubDocDeleted(MatchDoc matchDoc) const {
        MatchDoc firstSub = _first->get(matchDoc);
        return firstSub == matchdoc::INVALID_MATCHDOC;
    }

    template <typename T>
    void getSubDocValues(MatchDoc matchDoc, const Reference<T> *ref,
                         std::vector<T> &values) const;

private:
    const Reference<MatchDoc> *_first;
    Reference<MatchDoc> *_current;
    const Reference<MatchDoc> *_next;
private:
};

template<typename SubProcessor>
inline void SubDocAccessor::foreach(MatchDoc doc, SubProcessor &process) const {
    MatchDoc &current = _current->getReference(doc);
    const Reference<MatchDoc> *nextRef = _next;
    MatchDoc originCurrent = current;
    MatchDoc next = _first->get(doc);
    while (next != INVALID_MATCHDOC) {
        current = next;
        next = nextRef->get(current);
        process(doc);
    }
    current = originCurrent;
}

inline size_t SubDocAccessor::getSubDocIds(MatchDoc doc,
        std::vector<int32_t> &subDocids) const
{
    size_t count = 0;
    auto nextDoc = _first->get(doc);
    while (nextDoc != INVALID_MATCHDOC) {
        subDocids.push_back(nextDoc.getDocId());
        count++;
        nextDoc = _next->get(nextDoc);
    }
    return count;
}

inline void SubDocAccessor::setSubDocIds(MatchDoc doc,
        const std::vector<int32_t> &subDocids, size_t &beginPos)
{
    assert(beginPos >= 0);
    auto *nextDoc = _first->getPointer(doc);
    while (*nextDoc != INVALID_MATCHDOC
           && beginPos != subDocids.size()) {
        nextDoc->setDocId(subDocids[beginPos++]);
        nextDoc = _next->getPointer(*nextDoc);
    }
}

inline size_t SubDocAccessor::getSubDocs(MatchDoc doc,
        std::vector<MatchDoc> &subDocs) const
{
    size_t count = 0;
    auto nextDoc = _first->get(doc);
    while (nextDoc != INVALID_MATCHDOC) {
        subDocs.push_back(nextDoc);
        count++;
        nextDoc = _next->get(nextDoc);
    }
    return count;
}

inline void SubDocAccessor::setSubDocs(MatchDoc doc,
        const std::vector<MatchDoc> &subDocs, size_t &beginPos)
{
    assert(beginPos >= 0);
    auto *nextDoc = _first->getPointer(doc);
    while (*nextDoc != INVALID_MATCHDOC
           && beginPos != subDocs.size()) {
        *nextDoc = subDocs[beginPos++];
        nextDoc = _next->getPointer(*nextDoc);
    }

    auto &currentDoc = _current->getReference(doc);
    currentDoc = _first->get(doc);
}

template<typename Processor>
inline void SubDocAccessor::foreachFlatten(
        MatchDoc parentDoc, MatchDocAllocator *allocator,
        Processor &processor) const
{
    MatchDoc curSubDoc = _first->get(parentDoc);
    while (true) {
        MatchDoc nextSubDoc = _next->get(curSubDoc);
        if (nextSubDoc == matchdoc::INVALID_MATCHDOC) {
            break;
        }
        MatchDoc newDoc = allocator->allocateAndClone(parentDoc);
        _current->set(newDoc, curSubDoc);
        _first->set(newDoc, curSubDoc);
        _next->set(curSubDoc, matchdoc::INVALID_MATCHDOC);
        processor(newDoc);
        curSubDoc = nextSubDoc;
    }
    _current->set(parentDoc, curSubDoc);
    _first->set(parentDoc, curSubDoc);
    processor(parentDoc);
}

template <typename T>
inline void SubDocAccessor::getSubDocValues(
        MatchDoc matchDoc, const Reference<T> *subRef,
        std::vector<T> &values) const
{
    MatchDoc subDoc = _first->get(matchDoc);
    while (subDoc != matchdoc::INVALID_MATCHDOC) {
        T value = *subRef->getRealPointer(subDoc);
        values.push_back(value);
        MatchDoc nextSubDoc = _next->get(subDoc);
        subDoc = nextSubDoc;
    }
}

////////////////////////////////////////////////////////////
class SubCounter {
public:
    SubCounter()
        : _counter(0)
    {}
public:
    void operator()(MatchDoc doc) {
        ++_counter;
    }
    uint32_t get() const {
        return _counter;
    }
private:
    uint32_t _counter;
};

class SubDeleter {
public:
    SubDeleter(const Reference<MatchDoc> *current,
               MatchDocAllocator *subAllocator)
        : _current(current)
        , _subAllocator(subAllocator)
    {}
public:
    void operator()(MatchDoc doc) {
        MatchDoc subDoc = _current->get(doc);
        _subAllocator->deallocate(subDoc);
    }
private:
    const Reference<MatchDoc> *_current;
    MatchDocAllocator *_subAllocator;
};

class SubCloner {
public:
    SubCloner(const Reference<MatchDoc> *current,
            MatchDocAllocator *mainAllocator,
            MatchDocAllocator *subAllocator,
            MatchDoc newDoc)
        : _current(current)
        , _mainAllocator(mainAllocator)
        , _subAllocator(subAllocator)
        , _newDoc(newDoc) {}

public:
    void operator()(MatchDoc doc) {
        MatchDoc subDoc = _current->get(doc);
	MatchDoc newSubDoc = _subAllocator->allocateAndClone(subDoc);
	_mainAllocator->addSub(_newDoc, newSubDoc);
    }
private:
    const Reference<MatchDoc> *_current;
    MatchDocAllocator *_mainAllocator;
    MatchDocAllocator *_subAllocator;
    MatchDoc _newDoc;
};

}

#endif //ISEARCH_SUBDOCACCESSOR_H
