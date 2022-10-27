#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/common/CommonDef.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/GlobalIdentifier.h>
#include <ha3/rank/MatchData.h>
#include <ha3/rank/SimpleMatchData.h>
#include <ha3/rank/DistinctInfo.h>

using namespace matchdoc;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, Ha3MatchDocAllocator);

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
                auto refTyped = dynamic_cast<matchdoc::Reference<ItemType> *>(ref); \
                assert(refTyped);                                       \
                auto ret = new AttributeItemTyped<ItemType>();          \
                const auto &x = refTyped->get(matchDoc);                \
                ret->setItem(x);                                        \
                at.reset(ret);                                          \
            } else {                                                    \
                auto refTyped = dynamic_cast<matchdoc::Reference<ExprType> *>(ref); \
                assert(refTyped);                                       \
                auto ret = new AttributeItemTyped<ItemType>();          \
                const auto &x = refTyped->get(matchDoc);                \
                const ItemType &v = Type2AttrItemType<ExprType>::convert(x); \
                ret->setItem(v);                                        \
                at.reset(ret);                                          \
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
                auto refTyped = dynamic_cast<matchdoc::Reference<ItemType> *>(ref); \
                assert(refTyped);                                       \
                auto ret = new AttributeItemTyped<ItemType>();          \
                const auto &x = refTyped->get(matchDoc);                \
                ret->setItem(x);                                        \
                at.reset(ret);                                          \
            } else {                                                    \
                auto refTyped = dynamic_cast<matchdoc::Reference<ExprType> *>(ref); \
                assert(refTyped);                                       \
                auto ret = new AttributeItemTyped<ItemType>();          \
                const auto &x = refTyped->get(matchDoc);                \
                const ItemType &v = Type2AttrItemType<ExprType>::convert(x); \
                ret->setItem(v);                                        \
                at.reset(ret);                                          \
            }                                                           \
            break;                                                      \
        }                                                               \
    }
    AttributeItemPtr at;
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

    return at;
}

END_HA3_NAMESPACE(search);
