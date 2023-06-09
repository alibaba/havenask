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
#ifndef __INDEXLIB_COMPARATOR_H
#define __INDEXLIB_COMPARATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/doc_info.h"
#include "indexlib/index/merger_util/truncate/reference.h"
#include "indexlib/index/merger_util/truncate/reference_typed.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

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
        mRef = dynamic_cast<index::legacy::ReferenceTyped<T>*>(refer);
        assert(mRef);
        mDesc = desc;
    }

    ~ComparatorTyped() {}

public:
    bool LessThan(const DocInfo* left, const DocInfo* right) const override
    {
        T t1 {}, t2 {};
        bool null1 = false, null2 = false;
        mRef->Get(left, t1, null1);
        mRef->Get(right, t2, null2);
        if (!mDesc) {
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
    ReferenceTyped<T>* mRef;
    bool mDesc;
};

DEFINE_SHARED_PTR(Comparator);

struct DocInfoComparator {
public:
    DocInfoComparator() {}
    DocInfoComparator(const ComparatorPtr& comparator) : mComparator(comparator) {}

    bool operator()(const DocInfo* left, const DocInfo* right) { return mComparator->LessThan(left, right); }

private:
    ComparatorPtr mComparator;
};
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_COMPARATOR_H
