#include <ha3/sql/ops/join/ValueCollector.h>
#include <indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h>
#include <basic_ops/common/CommonDefine.h>

using namespace std;
using namespace matchdoc;
using namespace autil;
using namespace autil::mem_pool;
using namespace heavenask::indexlib;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, ValueCollector);

ValueCollector::ValueCollector() : _skeyRef(NULL) {}

ValueCollector::~ValueCollector() {}

bool ValueCollector::init(const IndexPartitionSchemaPtr& tableSchema,
                          const MatchDocAllocatorPtr& allocator) {
    assert(tableSchema);
    assert(allocator);
    AttributeSchemaPtr attributeSchema = tableSchema->GetAttributeSchema();
    uint32_t valueCount = attributeSchema->GetAttributeCount();
    bool isMountRef = true;
    if (tableSchema->GetTableType() == heavenask::indexlib::tt_kv) {
        if (valueCount == 1) {
            const AttributeConfigPtr& attrConfig = attributeSchema->GetAttributeConfig(0);
            isMountRef = attrConfig->IsMultiValue() || attrConfig->GetFieldType() == ft_string;
        }
    }
    for (uint32_t i = 0; i < valueCount; i++) {
        const AttributeConfigPtr& attrConfig = attributeSchema->GetAttributeConfig(i);
        assert(attrConfig);
        FieldType fieldType = attrConfig->GetFieldType();
        bool isMulti = attrConfig->IsMultiValue();
        std::string valueName = attrConfig->GetFieldConfig()->GetFieldName();
        ReferenceBase* fieldRef =
            declareReference(allocator, fieldType, isMulti, valueName, MOUNT_TABLE_GROUP);
        if (!fieldRef) {
            SQL_LOG(ERROR, "declare reference for value [%s] failed!", valueName.c_str());
            return false;
        }
        fieldRef->setSerializeLevel(SL_ATTRIBUTE);
        if (fieldRef->isMount() != isMountRef) {
            SingleFieldIndexConfigPtr pkIndexConfig =
                tableSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
            assert(pkIndexConfig);
            KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkIndexConfig);
            if (tableSchema->GetTableType() == heavenask::indexlib::tt_kkv &&
                valueName == kkvIndexConfig->GetSuffixFieldName()) {
                _skeyRef = fieldRef;  // skey value optimize store, use skey hash
                continue;
            }
            SQL_LOG(ERROR, "value [%s] should use %s reference!", valueName.c_str(),
                            isMountRef ? "mount" : "non-mount");
            return false;
        }
        _valueRefs.push_back(fieldRef);
    }
    return true;
}

// for kv collect
void ValueCollector::collectValueFields(matchdoc::MatchDoc matchDoc,
                                        const autil::ConstString& value) {
    assert(matchDoc != matchdoc::INVALID_MATCHDOC);
    assert(!_valueRefs.empty());
    if (_valueRefs[0]->isMount()) {
        _valueRefs[0]->mount(matchDoc, value.data());
    } else {
        setValue(matchDoc, _valueRefs[0], value.data());
    }
}

// for kkv collect
void ValueCollector::collectValueFields(matchdoc::MatchDoc matchDoc, KKVIterator* iter) {
    assert(iter && iter->IsValid());
    assert(matchDoc != matchdoc::INVALID_MATCHDOC);
    assert(!_valueRefs.empty() && _valueRefs[0]->isMount());
    autil::ConstString value;
    iter->GetCurrentValue(value);
    _valueRefs[0]->mount(matchDoc, value.data());
    if (_skeyRef) {
        if (!iter->IsOptimizeStoreSKey()) {
            SQL_LOG(ERROR, "skey ref is not NULL only when kkv skey isOpitmizeStoreSKey!");
            return;
        }
        uint64_t skeyHash = iter->GetCurrentSkey();
        matchdoc::ValueType valueType = _skeyRef->getValueType();
        assert(valueType.isBuiltInType());
        assert(!valueType.isMultiValue());
        matchdoc::BuiltinType fieldType = valueType.getBuiltinType();
        switch (fieldType) {
#define CASE_MACRO(ft)                                                                     \
    case ft: {                                                                             \
        typedef matchdoc::BuiltinType2CppType<ft>::CppType T;                              \
        matchdoc::Reference<T>* refTyped = static_cast<matchdoc::Reference<T>*>(_skeyRef); \
        refTyped->set(matchDoc, (T)skeyHash);                                              \
        break;                                                                             \
    }
            NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);  // matchdoc
#undef CASE_MACRO
            default: { assert(false); }
        }
    }
}

template <FieldType ft, bool isMulti>
struct IndexFieldType2CppType {
    struct UnknownType {};
    typedef UnknownType CppType;
};

#define IndexFieldType2CppTypeTraits(indexFieldType, isMulti, cppType) \
    template <>                                                        \
    struct IndexFieldType2CppType<indexFieldType, isMulti> {           \
        typedef cppType CppType;                                       \
    }
IndexFieldType2CppTypeTraits(ft_int8, false, int8_t);
IndexFieldType2CppTypeTraits(ft_int16, false, int16_t);
IndexFieldType2CppTypeTraits(ft_int32, false, int32_t);
IndexFieldType2CppTypeTraits(ft_int64, false, int64_t);
IndexFieldType2CppTypeTraits(ft_uint8, false, uint8_t);
IndexFieldType2CppTypeTraits(ft_uint16, false, uint16_t);
IndexFieldType2CppTypeTraits(ft_uint32, false, uint32_t);
IndexFieldType2CppTypeTraits(ft_uint64, false, uint64_t);
IndexFieldType2CppTypeTraits(ft_float, false, float);
IndexFieldType2CppTypeTraits(ft_double, false, double);
IndexFieldType2CppTypeTraits(ft_string, false, autil::MultiChar);
IndexFieldType2CppTypeTraits(ft_hash_64, false, uint64_t);
IndexFieldType2CppTypeTraits(ft_hash_128, false, autil::uint128_t);

IndexFieldType2CppTypeTraits(ft_int8, true, autil::MultiInt8);
IndexFieldType2CppTypeTraits(ft_int16, true, autil::MultiInt16);
IndexFieldType2CppTypeTraits(ft_int32, true, autil::MultiInt32);
IndexFieldType2CppTypeTraits(ft_int64, true, autil::MultiInt64);
IndexFieldType2CppTypeTraits(ft_uint8, true, autil::MultiUInt8);
IndexFieldType2CppTypeTraits(ft_uint16, true, autil::MultiUInt16);
IndexFieldType2CppTypeTraits(ft_uint32, true, autil::MultiUInt32);
IndexFieldType2CppTypeTraits(ft_uint64, true, autil::MultiUInt64);
IndexFieldType2CppTypeTraits(ft_float, true, autil::MultiFloat);
IndexFieldType2CppTypeTraits(ft_double, true, autil::MultiDouble);
IndexFieldType2CppTypeTraits(ft_string, true, autil::MultiString);
IndexFieldType2CppTypeTraits(ft_hash_64, true, autil::MultiUInt64);

#define FIELD_TYPE_MACRO_HELPER(MY_MACRO) \
    MY_MACRO(ft_int8);                    \
    MY_MACRO(ft_int16);                   \
    MY_MACRO(ft_int32);                   \
    MY_MACRO(ft_int64);                   \
    MY_MACRO(ft_uint8);                   \
    MY_MACRO(ft_uint16);                  \
    MY_MACRO(ft_uint32);                  \
    MY_MACRO(ft_uint64);                  \
    MY_MACRO(ft_float);                   \
    MY_MACRO(ft_double);                  \
    MY_MACRO(ft_string);                  \
    MY_MACRO(ft_hash_64);

matchdoc::ReferenceBase* ValueCollector::declareReference(
    const matchdoc::MatchDocAllocatorPtr& allocator, FieldType fieldType, bool isMulti,
    const std::string& valueName, const std::string& groupName) {
    switch (fieldType) {
#define CASE_MACRO(ft)                                            \
    case ft: {                                                    \
        if (isMulti) {                                            \
            typedef IndexFieldType2CppType<ft, true>::CppType T;  \
            return allocator->declare<T>(valueName, groupName);   \
        } else {                                                  \
            typedef IndexFieldType2CppType<ft, false>::CppType T; \
            return allocator->declare<T>(valueName, groupName);   \
        }                                                         \
    }
        FIELD_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
        case ft_hash_128: {
            if (isMulti) {
                return NULL;
            } else {
                typedef IndexFieldType2CppType<ft_hash_128, false>::CppType T;
                return allocator->declare<T>(valueName, groupName);
            }
        }
        default: { return NULL; }
    }
}

void ValueCollector::setValue(MatchDoc doc, ReferenceBase* ref, const char* value) {
    assert(ref && value);
    matchdoc::ValueType valueType = ref->getValueType();
    assert(valueType.isBuiltInType());
    matchdoc::BuiltinType fieldType = valueType.getBuiltinType();
    switch (fieldType) {
#define CASE_MACRO(ft)                                                  \
        case ft: {                                                      \
            typedef matchdoc::BuiltinType2CppType<ft>::CppType T;       \
            matchdoc::Reference<T>* refTyped = static_cast<matchdoc::Reference<T>*>(ref); \
            refTyped->set(doc, *(T*)value);                             \
            break;                                                      \
        }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: { assert(false); }
    }
}

END_HA3_NAMESPACE(sql);
