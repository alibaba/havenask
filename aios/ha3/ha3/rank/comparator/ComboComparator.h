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

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/rank/Comparator.h"
#include "matchdoc/MatchDoc.h"

namespace matchdoc {
template <typename T>
class Reference;
} // namespace matchdoc
namespace suez {
namespace turing {
template <typename T>
class AttributeExpressionTyped;
} // namespace turing
} // namespace suez

namespace isearch {
namespace rank {

class ComboComparator : public Comparator {
public:
    ComboComparator();
    ~ComboComparator();

public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override;
    std::string getType() const override {
        return "combo";
    }

public:
    void addComparator(const Comparator *cmp);
    uint32_t getComparatorCount() const {
        return _cmpVector.size();
    }
    void setExtrDocIdComparator(Comparator *cmp);
    void setExtrHashIdComparator(Comparator *cmp);
    void setExtrClusterIdComparator(Comparator *cmp);

protected:
    bool compareDocInfo(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const;

public:
    // for test
    typedef std::vector<const Comparator *> ComparatorVector;
    const ComparatorVector &getComparators() const {
        return _cmpVector;
    }

private:
    ComparatorVector _cmpVector;
    Comparator *_extrDocIdCmp;
    Comparator *_extrHashIdCmp;
    Comparator *_extrClusterIdCmp;

private:
    AUTIL_LOG_DECLARE();
};

//////optimized for comparator
template <typename T>
class OneRefComparatorTyped : public ComboComparator {
public:
    OneRefComparatorTyped(const matchdoc::Reference<T> *variableReference, bool sortFlag) {
        _reference = variableReference;
        _sortFlag = sortFlag;
    }

public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override {
        T &ta = *_reference->getPointer(a);
        T &tb = *_reference->getPointer(b);
        if (compareRef(ta, tb)) {
            return true;
        } else if (compareRef(tb, ta)) {
            return false;
        }
        return compareDocInfo(a, b);
    }

private:
    inline bool compareRef(T &a, T &b) const {
        return _sortFlag ? b < a : a < b;
    }
    std::string getType() const override {
        return "one_ref";
    }

private:
    const matchdoc::Reference<T> *_reference;
    bool _sortFlag;
};

template <typename T1, typename T2>
class TwoRefComparatorTyped : public ComboComparator {
public:
    TwoRefComparatorTyped(const matchdoc::Reference<T1> *variableReference1,
                          const matchdoc::Reference<T2> *variableReference2,
                          bool sortFlag1,
                          bool sortFlag2) {
        _reference1 = variableReference1;
        _reference2 = variableReference2;
        _sortFlag1 = sortFlag1;
        _sortFlag2 = sortFlag2;
    }

public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override {
        T1 &ta = *_reference1->getPointer(a);
        T1 &tb = *_reference1->getPointer(b);
        if (compareRef1(ta, tb)) {
            return true;
        } else if (compareRef1(tb, ta)) {
            return false;
        }
        T2 &tc = *_reference2->getPointer(a);
        T2 &td = *_reference2->getPointer(b);
        if (compareRef2(tc, td)) {
            return true;
        } else if (compareRef2(td, tc)) {
            return false;
        }
        return compareDocInfo(a, b);
    }

private:
    inline bool compareRef1(T1 &a, T1 &b) const {
        return _sortFlag1 ? b < a : a < b;
    }
    inline bool compareRef2(T2 &a, T2 &b) const {
        return _sortFlag2 ? b < a : a < b;
    }

    std::string getType() const override {
        return "two_ref";
    }

private:
    const matchdoc::Reference<T1> *_reference1;
    bool _sortFlag1;
    const matchdoc::Reference<T2> *_reference2;
    bool _sortFlag2;
};

template <typename T1, typename T2>
class RefAndExprComparatorTyped : public ComboComparator {
public:
    RefAndExprComparatorTyped(const matchdoc::Reference<T1> *variableReference,
                              suez::turing::AttributeExpressionTyped<T2> *expr,
                              bool sortFlag1,
                              bool sortFlag2) {
        _reference = variableReference;
        _expr = expr;
        _sortFlag1 = sortFlag1;
        _sortFlag2 = sortFlag2;
    }

public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override {
        T1 &ta = *_reference->getPointer(a);
        T1 &tb = *_reference->getPointer(b);
        if (compareRef(ta, tb)) {
            return true;
        } else if (compareRef(tb, ta)) {
            return false;
        }
        T2 tc = _expr->evaluateAndReturn(a);
        T2 td = _expr->evaluateAndReturn(b);

        if (compareExpr(tc, td)) {
            return true;
        } else if (compareExpr(td, tc)) {
            return false;
        }
        return compareDocInfo(a, b);
    }
    std::string getType() const override {
        return "ref_expr";
    }

private:
    inline bool compareRef(T1 &a, T1 &b) const {
        return _sortFlag1 ? b < a : a < b;
    }

    inline bool compareExpr(T2 &a, T2 &b) const {
        return _sortFlag2 ? b < a : a < b;
    }

private:
    const matchdoc::Reference<T1> *_reference;
    suez::turing::AttributeExpressionTyped<T2> *_expr;
    bool _sortFlag1;
    bool _sortFlag2;
};

template <typename T1, typename T2, typename T3>
class ThreeRefComparatorTyped : public ComboComparator {
public:
    ThreeRefComparatorTyped(const matchdoc::Reference<T1> *variableReference1,
                            const matchdoc::Reference<T2> *variableReference2,
                            const matchdoc::Reference<T3> *variableReference3,
                            bool sortFlag1,
                            bool sortFlag2,
                            bool sortFlag3) {
        _reference1 = variableReference1;
        _reference2 = variableReference2;
        _reference3 = variableReference3;
        _sortFlag1 = sortFlag1;
        _sortFlag2 = sortFlag2;
        _sortFlag3 = sortFlag3;
    }

public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override {
        T1 &ta = *_reference1->getPointer(a);
        T1 &tb = *_reference1->getPointer(b);
        if (compareRef1(ta, tb)) {
            return true;
        } else if (compareRef1(tb, ta)) {
            return false;
        }
        T2 &tc = *_reference2->getPointer(a);
        T2 &td = *_reference2->getPointer(b);
        if (compareRef2(tc, td)) {
            return true;
        } else if (compareRef2(td, tc)) {
            return false;
        }
        T3 &te = *_reference3->getPointer(a);
        T3 &tf = *_reference3->getPointer(b);
        if (compareRef3(te, tf)) {
            return true;
        } else if (compareRef3(tf, te)) {
            return false;
        }
        return compareDocInfo(a, b);
    }

private:
    inline bool compareRef1(T1 &a, T1 &b) const {
        return _sortFlag1 ? b < a : a < b;
    }
    inline bool compareRef2(T2 &a, T2 &b) const {
        return _sortFlag2 ? b < a : a < b;
    }
    inline bool compareRef3(T3 &a, T3 &b) const {
        return _sortFlag3 ? b < a : a < b;
    }

    std::string getType() const override {
        return "three_ref";
    }

private:
    const matchdoc::Reference<T1> *_reference1;
    bool _sortFlag1;
    const matchdoc::Reference<T2> *_reference2;
    bool _sortFlag2;
    const matchdoc::Reference<T3> *_reference3;
    bool _sortFlag3;
};

template <typename T1, typename T2, typename T3>
class TwoRefAndExprComparatorTyped : public ComboComparator {
public:
    TwoRefAndExprComparatorTyped(const matchdoc::Reference<T1> *variableReference1,
                                 const matchdoc::Reference<T2> *variableReference2,
                                 suez::turing::AttributeExpressionTyped<T3> *expr,
                                 bool sortFlag1,
                                 bool sortFlag2,
                                 bool sortFlag3) {
        _reference1 = variableReference1;
        _reference2 = variableReference2;
        _expr = expr;
        _sortFlag1 = sortFlag1;
        _sortFlag2 = sortFlag2;
        _sortFlag3 = sortFlag3;
    }

public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const override {
        T1 &ta = *_reference1->getPointer(a);
        T1 &tb = *_reference1->getPointer(b);
        if (compareRef1(ta, tb)) {
            return true;
        } else if (compareRef1(tb, ta)) {
            return false;
        }
        T2 &tc = *_reference2->getPointer(a);
        T2 &td = *_reference2->getPointer(b);
        if (compareRef2(tc, td)) {
            return true;
        } else if (compareRef2(td, tc)) {
            return false;
        }
        T3 te = _expr->evaluateAndReturn(a);
        T3 tf = _expr->evaluateAndReturn(b);
        if (compareExpr(te, tf)) {
            return true;
        } else if (compareExpr(tf, te)) {
            return false;
        }
        return compareDocInfo(a, b);
    }
    std::string getType() const override {
        return "two_ref_expr";
    }

private:
    inline bool compareRef1(T1 &a, T1 &b) const {
        return _sortFlag1 ? b < a : a < b;
    }

    inline bool compareRef2(T2 &a, T2 &b) const {
        return _sortFlag2 ? b < a : a < b;
    }

    inline bool compareExpr(T3 &a, T3 &b) const {
        return _sortFlag3 ? b < a : a < b;
    }

private:
    const matchdoc::Reference<T1> *_reference1;
    const matchdoc::Reference<T2> *_reference2;
    suez::turing::AttributeExpressionTyped<T3> *_expr;
    bool _sortFlag1;
    bool _sortFlag2;
    bool _sortFlag3;
};

} // namespace rank
} // namespace isearch
