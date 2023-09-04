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
#include "sql/ops/scan/MatchDocComparatorCreator.h"

#include <assert.h>
#include <cstddef>
#include <new>
#include <stdint.h>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/rank/ComboComparator.h"
#include "ha3/rank/ReferenceComparator.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "sql/common/Log.h"

namespace isearch {
namespace rank {
class Comparator;
} // namespace rank
} // namespace isearch

using namespace std;
using namespace matchdoc;

namespace sql {

MatchDocComparatorCreator::MatchDocComparatorCreator(autil::mem_pool::Pool *pool,
                                                     matchdoc::MatchDocAllocator *allocator)
    : _pool(pool)
    , _allocator(allocator) {}

MatchDocComparatorCreator::~MatchDocComparatorCreator() {}

isearch::rank::ComboComparator *
MatchDocComparatorCreator::createComparator(const vector<string> &refNames,
                                            const vector<bool> &orders) {
    assert(refNames.size() == orders.size());
    isearch::rank::ComboComparator *comboComp = nullptr;
    if (refNames.size() == 1) {
        comboComp = createOptimizedComparator(refNames[0], !orders[0]);
    } else if (refNames.size() == 2) {
        comboComp = createOptimizedComparator(refNames[0], refNames[1], !orders[0], !orders[1]);
    }
    if (comboComp == nullptr) {
        comboComp = POOL_NEW_CLASS(_pool, isearch::rank::ComboComparator);
        for (size_t i = 0; i < refNames.size(); i++) {
            auto *comp = createComparator(refNames[i], !orders[i]);
            if (comp == nullptr) {
                SQL_LOG(ERROR, "create combo comparator failed");
                return nullptr;
            }
            comboComp->addComparator(comp);
        }
    }
    return comboComp;
}

isearch::rank::ComboComparator *
MatchDocComparatorCreator::createOptimizedComparator(const std::string &refName, bool flag) {
    auto *ref = _allocator->findReferenceWithoutType(refName);
    if (ref == nullptr) {
        return nullptr;
    }
    ValueType vt = ref->getValueType();
    if (vt.isMultiValue()) {
        return nullptr;
    }
    auto type = vt.getBuiltinType();
    if (type == bt_int32) {
        return createComboComparatorTyped<int32_t>(ref, flag);
    } else if (type == bt_int64) {
        return createComboComparatorTyped<int64_t>(ref, flag);
    } else if (type == bt_double) {
        return createComboComparatorTyped<double>(ref, flag);
    } else {
        return nullptr;
    }
}

isearch::rank::ComboComparator *MatchDocComparatorCreator::createOptimizedComparator(
    const std::string &refName1, const std::string &refName2, bool flag1, bool flag2) {
    auto *ref1 = _allocator->findReferenceWithoutType(refName1);
    auto *ref2 = _allocator->findReferenceWithoutType(refName2);
    if (ref1 == nullptr || ref2 == nullptr) {
        return nullptr;
    }
    ValueType vt1 = ref1->getValueType();
    ValueType vt2 = ref2->getValueType();
    if (vt1.isMultiValue() || vt2.isMultiValue()) {
        return nullptr;
    }
    auto type1 = vt1.getBuiltinType();
    auto type2 = vt2.getBuiltinType();
    if (type1 == bt_int32 && type2 == bt_int32) {
        return createComboComparatorTyped<int32_t, int32_t>(ref1, ref2, flag1, flag2);
    } else if (type1 == bt_int32 && type2 == bt_int64) {
        return createComboComparatorTyped<int32_t, int64_t>(ref1, ref2, flag1, flag2);
    } else if (type1 == bt_int32 && type2 == bt_double) {
        return createComboComparatorTyped<int32_t, double>(ref1, ref2, flag1, flag2);
    } else if (type1 == bt_int64 && type2 == bt_int32) {
        return createComboComparatorTyped<int64_t, int32_t>(ref1, ref2, flag1, flag2);
    } else if (type1 == bt_int64 && type2 == bt_int64) {
        return createComboComparatorTyped<int64_t, int64_t>(ref1, ref2, flag1, flag2);
    } else if (type1 == bt_int64 && type2 == bt_double) {
        return createComboComparatorTyped<int64_t, double>(ref1, ref2, flag1, flag2);
    } else if (type1 == bt_double && type2 == bt_int32) {
        return createComboComparatorTyped<double, int32_t>(ref1, ref2, flag1, flag2);
    } else if (type1 == bt_double && type2 == bt_int64) {
        return createComboComparatorTyped<double, int64_t>(ref1, ref2, flag1, flag2);
    } else if (type1 == bt_double && type2 == bt_double) {
        return createComboComparatorTyped<double, double>(ref1, ref2, flag1, flag2);
    } else {
        return nullptr;
    }
}

isearch::rank::Comparator *MatchDocComparatorCreator::createComparator(const std::string &refName,
                                                                       bool sortFlag) {
#define COMPARATOR_CREATOR_HELPER(vt_type)                                                         \
    case vt_type: {                                                                                \
        isearch::rank::Comparator *comp = nullptr;                                                 \
        if (isMulti) {                                                                             \
            typedef MatchDocBuiltinType2CppType<vt_type, true>::CppType T;                         \
            comp = createComparatorTyped<T>(ref, sortFlag);                                        \
        } else {                                                                                   \
            typedef MatchDocBuiltinType2CppType<vt_type, false>::CppType T;                        \
            comp = createComparatorTyped<T>(ref, sortFlag);                                        \
        }                                                                                          \
        if (comp == nullptr) {                                                                     \
            SQL_LOG(ERROR,                                                                         \
                    "create sort comparator failed, field [%s] type [%s%s]",                       \
                    refName.c_str(),                                                               \
                    isMulti ? "array " : "",                                                       \
                    builtinTypeToString(vt.getBuiltinType()).c_str());                             \
        }                                                                                          \
        return comp;                                                                               \
    }

    auto *ref = _allocator->findReferenceWithoutType(refName);
    if (ref == nullptr) {
        SQL_LOG(ERROR, "unexpected not find column [%s] in table", refName.c_str());
        return nullptr;
    }
    auto vt = ref->getValueType();
    bool isMulti = vt.isMultiValue();
    switch (vt.getBuiltinType()) {
        BUILTIN_TYPE_MACRO_HELPER(COMPARATOR_CREATOR_HELPER);
#undef COMPARATOR_CREATOR_HELPER
    default:
        SQL_LOG(ERROR,
                "create sort comparator failed, unexpected field [%s] type [%s%s]",
                refName.c_str(),
                isMulti ? "array " : "",
                builtinTypeToString(vt.getBuiltinType()).c_str());
        assert(false);
        return nullptr;
    }
}

template <typename T>
isearch::rank::ComboComparator *
MatchDocComparatorCreator::createComboComparatorTyped(ReferenceBase *ref, bool flag) {
    Reference<T> *refTyped = dynamic_cast<Reference<T> *>(ref);
    if (refTyped == nullptr) {
        SQL_LOG(WARN, "unexpected cast ref [%s] failed", ref->getName().c_str());
        return nullptr;
    }
    return POOL_NEW_CLASS(_pool, isearch::rank::OneRefComparatorTyped<T>, refTyped, flag);
}

template <typename T1, typename T2>
isearch::rank::ComboComparator *MatchDocComparatorCreator::createComboComparatorTyped(
    ReferenceBase *ref1, ReferenceBase *ref2, bool sortFlag1, bool sortFlag2) {
    Reference<T1> *refTyped1 = dynamic_cast<Reference<T1> *>(ref1);
    if (refTyped1 == nullptr) {
        SQL_LOG(WARN, "unexpected cast ref [%s] failed", ref1->getName().c_str());
        return nullptr;
    }
    Reference<T2> *refTyped2 = dynamic_cast<Reference<T2> *>(ref2);
    if (refTyped2 == nullptr) {
        SQL_LOG(WARN, "unexpected cast ref [%s] failed", ref2->getName().c_str());
        return nullptr;
    }
    return new (_pool->allocate(sizeof(isearch::rank::TwoRefComparatorTyped<T1, T2>)))
        isearch::rank::TwoRefComparatorTyped<T1, T2>(refTyped1, refTyped2, sortFlag1, sortFlag2);
}

template <typename T>
isearch::rank::Comparator *MatchDocComparatorCreator::createComparatorTyped(ReferenceBase *ref,
                                                                            bool flag) {
    Reference<T> *refTyped = dynamic_cast<Reference<T> *>(ref);
    if (refTyped == nullptr) {
        SQL_LOG(WARN, "unexpected cast ref [%s] failed", ref->getName().c_str());
        return nullptr;
    }
    return POOL_NEW_CLASS(_pool, isearch::rank::ReferenceComparator<T>, refTyped, flag);
}

} // namespace sql
