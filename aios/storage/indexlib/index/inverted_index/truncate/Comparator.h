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

#include <assert.h>
#include <memory>

#include "indexlib/index/inverted_index/truncate/DocInfo.h"
#include "indexlib/index/inverted_index/truncate/Reference.h"
#include "indexlib/index/inverted_index/truncate/ReferenceTyped.h"

namespace indexlib::index {

class Comparator
{
public:
    Comparator() {}
    virtual ~Comparator() {}

public:
    bool operator()(const DocInfo* left, const DocInfo* right) { return LessThan(left, right); }

    virtual bool LessThan(const DocInfo* left, const DocInfo* right) const = 0;
};

template <typename T>
class ComparatorTyped : public Comparator
{
public:
    ComparatorTyped(Reference* refer, bool desc)
    {
        _ref = dynamic_cast<ReferenceTyped<T>*>(refer);
        assert(_ref);
        _desc = desc;
    }

    ~ComparatorTyped() = default;

public:
    bool LessThan(const DocInfo* left, const DocInfo* right) const override
    {
        T t1 {}, t2 {};
        bool null1 = false, null2 = false;
        _ref->Get(left, t1, null1);
        _ref->Get(right, t2, null2);
        if (!_desc) {
            // asc
            if (null1 && null2) {
                return false;
            }
            if (null1) {
                return true;
            }
            if (null2) {
                return false;
            }
            return t1 < t2;
        } else {
            // desc
            if (null1) {
                return false;
            }
            if (null2) {
                return true;
            }
            return t1 > t2;
        }
    }

private:
    ReferenceTyped<T>* _ref;
    bool _desc;
};

struct DocInfoComparator {
public:
    DocInfoComparator() {}
    DocInfoComparator(const std::shared_ptr<Comparator>& comparator) : _comparator(comparator) {}

    bool operator()(const DocInfo* left, const DocInfo* right) { return _comparator->LessThan(left, right); }

private:
    std::shared_ptr<Comparator> _comparator;
};

} // namespace indexlib::index
