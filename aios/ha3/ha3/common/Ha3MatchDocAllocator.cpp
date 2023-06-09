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
#include "ha3/common/Ha3MatchDocAllocator.h"

#include <algorithm>
#include <type_traits>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/LongHashValue.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/common/GlobalIdentifier.h"
#include "ha3/common/ReferenceIdManager.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/rank/DistinctInfo.h"
#include "ha3/search/MatchData.h"
#include "ha3/search/SimpleMatchData.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace matchdoc;
using namespace suez::turing;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, Ha3MatchDocAllocator);

Ha3MatchDocAllocator::Ha3MatchDocAllocator(autil::mem_pool::Pool *pool,
        bool useSubDoc)
    : MatchDocAllocator(pool, useSubDoc)
    , _refIdManager(new ReferenceIdManager())

{
    registerTypes();
    //setDefaultGroupName(HA3_DEFAULT_GROUP);
}

Ha3MatchDocAllocator::~Ha3MatchDocAllocator() {
    DELETE_AND_SET_NULL(_refIdManager);
}

void Ha3MatchDocAllocator::registerTypes() {
    registerType<Tracer>();
    registerType<rank::MatchData>();
    registerType<rank::SimpleMatchData>();
    registerType<rank::DistinctInfo>();
}

uint32_t Ha3MatchDocAllocator::requireReferenceId(const std::string &refName) {
    return _refIdManager->requireReferenceId(getReferenceName(refName));
}

void Ha3MatchDocAllocator::initPhaseOneInfoReferences(uint8_t phaseOneInfoFlag) {
    if (PHASE_ONE_HAS_PK64(phaseOneInfoFlag)) {
        declareWithConstructFlagDefaultGroup<uint64_t>(PRIMARYKEY_REF,
                false, SL_CACHE);
    }
    if (PHASE_ONE_HAS_PK128(phaseOneInfoFlag)) {
        declareWithConstructFlagDefaultGroup<primarykey_t>(PRIMARYKEY_REF,
                true, SL_CACHE);
    }
}

common::AttributeItemPtr Ha3MatchDocAllocator::createAttributeItem(
        matchdoc::ReferenceBase *ref,
        matchdoc::MatchDoc matchDoc)
{
#define ATTR_ITEM_CASE_HELPER(vt_type)                                  \
    case vt_type:                                                       \
    {                                                                   \
        if (isMulti) {                                                  \
            typedef VariableTypeTraits<vt_type, true>::AttrItemType ItemType; \
            typedef VariableTypeTraits<vt_type, true>::AttrExprType ExprType; \
            if (!std::is_same<matchdoc::SupportSerializeTrait<ItemType>::type, \
                    matchdoc::SupportSerializeType>::value) {           \
                return AttributeItemPtr();                              \
            }                                                           \
            if (isStdType) {                                            \
                auto refTyped = static_cast<matchdoc::Reference<ItemType> *>(ref); \
                const auto &x = refTyped->get(matchDoc);                \
                AttributeItemPtr at(new AttributeItemTyped<ItemType>(x)); \
                return at;                                              \
            } else {                                                    \
                auto refTyped = static_cast<matchdoc::Reference<ExprType> *>(ref); \
                const auto &x = refTyped->get(matchDoc);                \
                const ItemType &v = Type2AttrItemType<ExprType>::convert(x); \
                AttributeItemPtr at(new AttributeItemTyped<ItemType>(v)); \
                return at;                                              \
            }                                                           \
            break;                                                      \
        } else {                                                        \
            typedef VariableTypeTraits<vt_type, false>::AttrItemType ItemType; \
            typedef VariableTypeTraits<vt_type, false>::AttrExprType ExprType; \
            if (!std::is_same<matchdoc::SupportSerializeTrait<ItemType>::type, \
                    matchdoc::SupportSerializeType>::value) {           \
                return AttributeItemPtr();                              \
            }                                                           \
            if (isStdType) {                                            \
                auto refTyped = static_cast<matchdoc::Reference<ItemType> *>(ref); \
                const auto &x = refTyped->get(matchDoc);                \
                AttributeItemPtr at(new AttributeItemTyped<ItemType>(x)); \
                return at;                                              \
            } else {                                                    \
                auto refTyped = static_cast<matchdoc::Reference<ExprType> *>(ref); \
                const auto &x = refTyped->get(matchDoc);                \
                const ItemType &v = Type2AttrItemType<ExprType>::convert(x); \
                AttributeItemPtr at(new AttributeItemTyped<ItemType>(v)); \
                return at;                                              \
            }                                                           \
            break;                                                      \
        }                                                               \
    }

    auto valueType = ref->getValueType();
    auto vt = valueType.getBuiltinType();
    auto isMulti = valueType.isMultiValue();
    auto isStdType = valueType.isStdType();
    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(ATTR_ITEM_CASE_HELPER);
    default:
        break;
    }
#undef ATTR_TYPE_CASE_HELPE

    return AttributeItemPtr();
}

} // namespace search
} // namespace isearch
