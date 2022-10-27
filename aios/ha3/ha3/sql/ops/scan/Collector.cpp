#include <ha3/sql/ops/scan/Collector.h>
#include <indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h>
#include <suez/turing/expression/util/TypeTransformer.h>

using namespace std;
using namespace matchdoc;
using namespace autil;
using namespace autil::mem_pool;
using namespace heavenask::indexlib;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(config);

BEGIN_HA3_NAMESPACE(sql);

////////////////////////////////////////////////////////////////////////
HA3_LOG_SETUP(sql, KeyCollector);

KeyCollector::KeyCollector()
    : _pkeyBuiltinType(bt_unknown), _pkeyRef(NULL)
{}

KeyCollector::~KeyCollector() {}

bool KeyCollector::init(const IndexPartitionSchemaPtr& tableSchema,
                        const MatchDocAllocatorPtr& mountedAllocator)
{
    assert(tableSchema);
    assert(mountedAllocator);
    // init pk
    IndexSchemaPtr indexSchema = tableSchema->GetIndexSchema();
    string pkFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
    FieldSchemaPtr fieldSchema = tableSchema->GetFieldSchema();
    FieldConfigPtr pkFieldConfig = fieldSchema->GetFieldConfig(pkFieldName);
    assert(pkFieldConfig);
    AttributeSchemaPtr attributeSchema = tableSchema->GetAttributeSchema();
    if(NULL != attributeSchema->GetAttributeConfig(pkFieldName)) {
        _pkeyBuiltinType = suez::turing::TypeTransformer::transform(pkFieldConfig->GetFieldType());
        return true;
    }
    ReferenceBase *pkeyRef = CollectorUtil::declareReference(mountedAllocator, pkFieldConfig->GetFieldType(),
                                                             false, pkFieldName,
                                                             CollectorUtil::MOUNT_TABLE_GROUP);
    if (!pkeyRef) {
        SQL_LOG(ERROR, "declare reference for pkey [%s] failed!", pkFieldName.c_str());
        return false;
    }
    matchdoc::ValueType valueType = pkeyRef->getValueType();
    if (unlikely(!valueType.isBuiltInType() || valueType.isMultiValue())) {
        return false;
    }
    matchdoc::BuiltinType pkeyBt = valueType.getBuiltinType();
    switch(pkeyBt) {
#define PKEY_CASE_CLAUSE(key_t)            \
    case key_t: {                          \
        break;                             \
    }
    NUMBER_BUILTIN_TYPE_MACRO_HELPER(PKEY_CASE_CLAUSE);
#undef PKEY_CASE_CLAUSE
    case bt_string: {
        break;
    }
    default:
        SQL_LOG(ERROR, "unsupported type for pkey [%s]", pkFieldName.c_str());
        return false;
    }
    pkeyRef->setSerializeLevel(SL_ATTRIBUTE);
    _pkeyBuiltinType = pkeyBt;
    _pkeyRef = pkeyRef;
    return true;
}

////////////////////////////////////////////////////////////////////////
HA3_LOG_SETUP(sql, ValueCollector);

ValueCollector::ValueCollector()
    : _skeyRef(NULL), _tsRef(NULL)
{}

ValueCollector::~ValueCollector() {}

bool ValueCollector::init(const IndexPartitionSchemaPtr& tableSchema,
                          const MatchDocAllocatorPtr& mountedAllocator)
{
    assert(tableSchema);
    assert(mountedAllocator);
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
        const string &valueName = attrConfig->GetFieldConfig()->GetFieldName();
        ReferenceBase* fieldRef = CollectorUtil::declareReference(
            mountedAllocator, fieldType, isMulti, valueName, CollectorUtil::MOUNT_TABLE_GROUP);
        if (!fieldRef) {
            SQL_LOG(ERROR, "declare reference for value [%s] failed!", valueName.c_str());
            return false;
        }
        if (fieldRef->isMount() != isMountRef) {
            SingleFieldIndexConfigPtr pkIndexConfig =
                tableSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
            assert(pkIndexConfig);
            // KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkIndexConfig);
            // if (tableSchema->GetTableType() == heavenask::indexlib::tt_kkv &&
            //     valueName == kkvIndexConfig->GetSuffixFieldName()) {
            //     fieldRef->setSerializeLevel(SL_ATTRIBUTE);
            //     _skeyRef = fieldRef;  // skey value optimize store, use skey hash
            //     continue;
            // }
            SQL_LOG(ERROR, "value [%s] should use %s reference!", valueName.c_str(),
                      isMountRef ? "mount" : "non-mount");
            return false;
        }
        fieldRef->setSerializeLevel(SL_ATTRIBUTE);
        _valueRefs.push_back(fieldRef);
    }
    return true;
}

// for kv collect
void ValueCollector::collectFields(matchdoc::MatchDoc matchDoc,
                                   const autil::ConstString& value)
{
    assert(matchDoc != matchdoc::INVALID_MATCHDOC);
    assert(!_valueRefs.empty());
    if (_valueRefs[0]->isMount()) {
        _valueRefs[0]->mount(matchDoc, value.data());
    } else {
        setValue(matchDoc, _valueRefs[0], value.data());
    }
}

// bool InitTimestampReference(const matchdoc::MatchDocAllocatorPtr& allocator,
//                             const string& refName)
// {
//     if (_tsRef)
//     {
//         matchdoc::Reference<uint64_t>* tsRef = allocator->findReference<uint64_t>(refName);
//         return tsRef == _tsRef;
//     }

//     _tsRef = allocator->declare<uint64_t>(refName);
//     return _tsRef != NULL;
// }

// bool NeedCollectTimestamp() const { return _tsRef != NULL; }
// void CollectTimestamp(matchdoc::MatchDoc matchDoc,
//                       IE_NAMESPACE(index)::KKVIterator* iter) __attribute__((always_inline));
// matchdoc::Reference<uint64_t>* GetTimestampRef() const { return _tsRef; }

void ValueCollector::setValue(MatchDoc doc, ReferenceBase* ref, const char* value)
{
    assert(ref && value);
    matchdoc::ValueType valueType = ref->getValueType();
    assert(valueType.isBuiltInType() && !valueType.isMultiValue());
    matchdoc::BuiltinType fieldType = valueType.getBuiltinType();
    switch (fieldType) {
#define CASE_MACRO(ft)                                                                \
    case ft: {                                                                        \
        typedef matchdoc::BuiltinType2CppType<ft>::CppType T;                         \
        matchdoc::Reference<T>* refTyped = static_cast<matchdoc::Reference<T>*>(ref); \
        refTyped->set(doc, *(T*)value);                                               \
        break;                                                                        \
    }
    NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: { assert(false); }
    }
}

// inline void ValueCollector::CollectTimestamp(
//         matchdoc::MatchDoc matchDoc, IE_NAMESPACE(index)::KKVIterator* iter)
// {
//     assert(_tsRef);
//     assert(iter && iter->IsValid());
//     assert(matchDoc != matchdoc::INVALID_MoATCHDOC);
//     _tsRef->set(matchDoc, iter->GetCurrentTimestamp());
// }


////////////////////////////////////////////////////////////////////////
HA3_LOG_SETUP(sql, CollectorUtil);

const string CollectorUtil::MOUNT_TABLE_GROUP = "__mount_table__";

MatchDocAllocatorPtr
CollectorUtil::createMountedMatchDocAllocator(const IndexPartitionSchemaPtr &schema,
                                              autil::mem_pool::Pool *pool)
{
    MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
    return allocator;
}

MountInfoPtr CollectorUtil::createPackAttributeMountInfo(const IndexPartitionSchemaPtr &schema)
{
    string tableName = schema->GetSchemaName();
    MountInfoPtr mountInfo(new MountInfo());
    uint32_t beginMountId = 0;
    CollectorUtil::insertPackAttributeMountInfo(mountInfo, schema, tableName, beginMountId);
    return mountInfo;
}

void CollectorUtil::insertPackAttributeMountInfo(const MountInfoPtr& singleMountInfo,
                                                 const IndexPartitionSchemaPtr& schema,
                                                 const string& tableName, uint32_t& beginMountId)
{
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }

    // heavenask::indexlib::TableType tableType = schema->GetTableType();
    // if (tableType == heavenask::indexlib::tt_kv || tableType== heavenask::indexlib::tt_kkv) {
    //     SingleFieldIndexConfigPtr pkIndexConfig =
    //             schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    //     assert(pkIndexConfig);

    //     KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkIndexConfig);
    //     assert(kvIndexConfig);
    //     const ValueConfigPtr& valueConfig = kvIndexConfig->GetValueConfig();
    //     assert(valueConfig);
    //     PackAttributeConfigPtr packConf = valueConfig->CreatePackAttributeConfig();
    //     if (tableType == heavenask::indexlib::tt_kv)
    //     {
    //         // TODO: kkv & kv should use same strategy later
    //         if (valueConfig->GetAttributeCount() == 1
    //             && !valueConfig->GetAttributeConfig(0)->IsMultiValue()
    //             && (valueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string))
    //         {
    //             packConf.reset();
    //         }
    //     }
    //     insertPackAttributeMountInfo(singleMountInfo, packConf, tableName, beginMountId++);
    //     return;
    // }

    for (size_t i = 0; i < attrSchema->GetPackAttributeCount(); i++)
    {
        const PackAttributeConfigPtr& packAttrConf =
                attrSchema->GetPackAttributeConfig((packattrid_t)i);
        assert(packAttrConf);
        insertPackAttributeMountInfo(singleMountInfo, packAttrConf,
                                     tableName, beginMountId++);
    }
}

void CollectorUtil::insertPackAttributeMountInfo(const MountInfoPtr& singleMountInfo,
                                                 const PackAttributeConfigPtr& packAttrConf,
                                                 const string& tableName, uint32_t mountId)
{
    if (!packAttrConf) {
        return;
    }
    vector<string> subAttrNames;
    packAttrConf->GetSubAttributeNames(subAttrNames);

    PackAttributeFormatter packAttrFormatter;
    if (!packAttrFormatter.Init(packAttrConf)) {
        SQL_LOG(ERROR, "table [%s] init pack attribute formatter failed!", tableName.c_str());
        return ;
    }

    for (size_t i = 0; i < subAttrNames.size(); i++) {
        AttributeReference* attrRef =
                packAttrFormatter.GetAttributeReference(subAttrNames[i]);
        assert(attrRef);

        string fullName = tableName + ":" + subAttrNames[i];
        singleMountInfo->insert(fullName, mountId, attrRef->GetOffset());
        singleMountInfo->insert(subAttrNames[i], mountId, attrRef->GetOffset());
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

matchdoc::ReferenceBase* CollectorUtil::declareReference(
    const matchdoc::MatchDocAllocatorPtr& allocator, FieldType fieldType, bool isMulti,
    const string& valueName, const string& groupName)
{
    switch (fieldType) {
#define CASE_MACRO(ft)                                            \
    case ft: {                                                    \
        if (isMulti) {                                            \
            typedef IndexFieldType2CppType<ft, true>::CppType T;  \
            return allocator->declare<T>(valueName, groupName); \
        } else {                                                  \
            typedef IndexFieldType2CppType<ft, false>::CppType T; \
            return allocator->declare<T>(valueName, groupName); \
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

END_HA3_NAMESPACE(sql);
