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
#include "sql/ops/scan/Collector.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/LongHashValue.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/schema_modify_operation.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/attribute/Types.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/pack_attribute/AttributeReference.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/kkv_iterator.h"
#include "indexlib/index/kkv/suffix_key_writer.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/util/Status2Exception.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "sql/common/Log.h"
#include "suez/turing/expression/util/TypeTransformer.h"

using namespace std;
using namespace matchdoc;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::index;
using namespace indexlib::config;

namespace sql {

KeyCollector::KeyCollector()
    : _pkeyBuiltinType(bt_unknown)
    , _pkeyRef(NULL) {}

KeyCollector::~KeyCollector() {}

bool KeyCollector::init(const std::shared_ptr<indexlibv2::config::ITabletSchema> &tableSchema,
                        const MatchDocAllocatorPtr &mountedAllocator) {
    assert(tableSchema);
    assert(mountedAllocator);
    const auto &tableType = tableSchema->GetTableType();
    if (!(tableType == indexlib::table::TABLE_TYPE_KKV
          || tableType == indexlib::table::TABLE_TYPE_KV
          || tableType == indexlib::table::TABLE_TYPE_AGGREGATION)) {
        SQL_LOG(ERROR, "not support table type [%s]", tableType.c_str());
        return false;
    }
    FieldType pkFieldType = ft_unknown;
    std::string pkFieldName;
    std::shared_ptr<indexlibv2::config::ValueConfig> valueConfig;
    if (!extractInfo(tableSchema, &pkFieldType, &pkFieldName, &valueConfig)) {
        SQL_LOG(ERROR, "get primary key index info failed");
        return false;
    }

    if (isFieldInValue(pkFieldName, valueConfig)) {
        _pkeyBuiltinType = suez::turing::TypeTransformer::transform(pkFieldType);
        return true;
    }

    ReferenceBase *pkeyRef = CollectorUtil::declareReference(
        mountedAllocator, pkFieldType, false, pkFieldName, CollectorUtil::MOUNT_TABLE_GROUP);
    if (!pkeyRef) {
        SQL_LOG(ERROR, "declare reference for pkey [%s] failed!", pkFieldName.c_str());
        return false;
    }
    matchdoc::ValueType valueType = pkeyRef->getValueType();
    if (unlikely(!valueType.isBuiltInType() || valueType.isMultiValue())) {
        return false;
    }
    matchdoc::BuiltinType pkeyBt = valueType.getBuiltinType();
    switch (pkeyBt) {
#define PKEY_CASE_CLAUSE(key_t)                                                                    \
    case key_t: {                                                                                  \
        break;                                                                                     \
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

bool KeyCollector::extractInfo(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &tableSchema,
    FieldType *pkFieldType,
    std::string *pkFieldName,
    std::shared_ptr<indexlibv2::config::ValueConfig> *valueConfig) {
    auto tableType = tableSchema->GetTableType();
    const auto &pkConfig = tableSchema->GetPrimaryKeyIndexConfig();
    if (!pkConfig) {
        SQL_LOG(ERROR,
                "get primary key index config failed, table_name[%s] table_type[%s]",
                tableSchema->GetTableName().c_str(),
                tableType.c_str());
        return false;
    }
    auto fieldConfigs = pkConfig->GetFieldConfigs();
    if (fieldConfigs.size() != 1u) {
        SQL_LOG(ERROR,
                "primary key index config[%s] field size[%lu] error",
                pkConfig->GetIndexName().c_str(),
                fieldConfigs.size());
        return false;
    }
    const auto &pkFieldConfig = fieldConfigs[0];
    *pkFieldType = pkFieldConfig->GetFieldType();
    *pkFieldName = pkFieldConfig->GetFieldName();

    if (tableType == indexlib::table::TABLE_TYPE_KKV) {
        const auto &kkvIndexConfig
            = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(pkConfig);
        assert(kkvIndexConfig);
        *valueConfig = kkvIndexConfig->GetValueConfig();
    } else {
        const auto &kvIndexConfigV2
            = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(pkConfig);
        assert(kvIndexConfigV2);
        *valueConfig = kvIndexConfigV2->GetValueConfig();
    }
    return true;
}

bool KeyCollector::isFieldInValue(
    const std::string &fieldName,
    const std::shared_ptr<indexlibv2::config::ValueConfig> &valueConfig) {
    size_t attrCount = valueConfig->GetAttributeCount();
    for (size_t i = 0; i < attrCount; ++i) {
        if (valueConfig->GetAttributeConfig(/*idx*/ i)->GetIndexName() == fieldName) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////

ValueCollector::ValueCollector()
    : _skeyRef(NULL) {}

ValueCollector::~ValueCollector() {}

bool ValueCollector::init(const IndexPartitionSchemaPtr &tableSchema,
                          const MatchDocAllocatorPtr &mountedAllocator) {
    assert(tableSchema);
    assert(mountedAllocator);
    AttributeSchemaPtr attributeSchema = tableSchema->GetAttributeSchema();
    uint32_t valueCount = attributeSchema->GetAttributeCount();
    bool isMountRef = true;
    if (tableSchema->GetTableType() == indexlib::tt_kv) {
        if (valueCount == 1) {
            const AttributeConfigPtr &attrConfig = attributeSchema->GetAttributeConfig(0);
            isMountRef = attrConfig->IsMultiValue() || attrConfig->GetFieldType() == ft_string;
        }
    }
    for (uint32_t i = 0; i < valueCount; i++) {
        const AttributeConfigPtr &attrConfig = attributeSchema->GetAttributeConfig(i);
        assert(attrConfig);
        FieldType fieldType = attrConfig->GetFieldType();
        bool isMulti = attrConfig->IsMultiValue();
        const string &valueName = attrConfig->GetFieldConfig()->GetFieldName();
        SQL_LOG(TRACE3, "declare value reference index[%u] name[%s]", i, valueName.c_str());
        ReferenceBase *fieldRef = CollectorUtil::declareReference(
            mountedAllocator, fieldType, isMulti, valueName, CollectorUtil::MOUNT_TABLE_GROUP);
        if (!fieldRef) {
            SQL_LOG(ERROR, "declare reference for value [%s] failed!", valueName.c_str());
            return false;
        }
        if (fieldRef->isMount() != isMountRef) {
            SingleFieldIndexConfigPtr pkIndexConfig
                = tableSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
            assert(pkIndexConfig);
            KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, pkIndexConfig);
            if (tableSchema->GetTableType() == indexlib::tt_kkv
                && valueName == kkvIndexConfig->GetSuffixFieldName()) {
                fieldRef->setSerializeLevel(SL_ATTRIBUTE);
                _skeyRef = fieldRef; // skey value optimize store, use skey hash
                continue;
            }
            SQL_LOG(ERROR,
                    "value [%s] should use %s reference!",
                    valueName.c_str(),
                    isMountRef ? "mount" : "non-mount");
            return false;
        }
        fieldRef->setSerializeLevel(SL_ATTRIBUTE);
        _valueRefs.push_back(fieldRef);
    }
    return true;
}

bool ValueCollector::init(const std::shared_ptr<indexlibv2::config::ValueConfig> &valueConfig,
                          const MatchDocAllocatorPtr &mountedAllocator,
                          const std::string &tableType) {
    assert(mountedAllocator);
    bool isMountRef = true;
    std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> attrFields(
        valueConfig->GetAttributeCount());
    const auto &attrConfigs = valueConfig->GetAttributeConfigs();
    std::transform(attrConfigs.begin(),
                   attrConfigs.end(),
                   attrFields.begin(),
                   [](const auto &attrConfig) { return attrConfig->GetFieldConfig(); });

    if (attrFields.size() == 1 && tableType == indexlib::table::TABLE_TYPE_KV) {
        const auto &attrConfig = attrConfigs[0];
        isMountRef = attrConfig->IsMultiValue() || attrConfig->GetFieldType() == ft_string;
    }

    for (const auto &fieldConfig : attrFields) {
        FieldType fieldType = fieldConfig->GetFieldType();
        bool isMulti = fieldConfig->IsMultiValue();
        const string &valueName = fieldConfig->GetFieldName();

        SQL_LOG(TRACE3, "declare value reference name[%s]", valueName.c_str());
        ReferenceBase *fieldRef = CollectorUtil::declareReference(
            mountedAllocator, fieldType, isMulti, valueName, CollectorUtil::MOUNT_TABLE_GROUP);
        if (!fieldRef) {
            SQL_LOG(ERROR, "declare reference for value [%s] failed!", valueName.c_str());
            return false;
        }
        if (fieldRef->isMount() != isMountRef) {
            SQL_LOG(ERROR,
                    "value [%s] should use %s reference!",
                    valueName.c_str(),
                    isMountRef ? "mount" : "non-mount");
            return false;
        }
        fieldRef->setSerializeLevel(SL_ATTRIBUTE);
        _valueRefs.push_back(fieldRef);
    }
    return true;
}

// for kkv collect
void ValueCollector::collectFields(matchdoc::MatchDoc matchDoc, KKVIterator *iter) {
    assert(iter && iter->IsValid());
    assert(matchDoc != matchdoc::INVALID_MATCHDOC);
    assert(!_valueRefs.empty() && _valueRefs[0]->isMount());
    autil::StringView value;
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
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        typedef matchdoc::BuiltinType2CppType<ft>::CppType T;                                      \
        matchdoc::Reference<T> *refTyped = static_cast<matchdoc::Reference<T> *>(_skeyRef);        \
        refTyped->set(matchDoc, (T)skeyHash);                                                      \
        break;                                                                                     \
    }
            NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO); // matchdoc
#undef CASE_MACRO
        default: {
            assert(false);
        }
        }
    }
}

// for kv collect
void ValueCollector::collectFields(matchdoc::MatchDoc matchDoc, const autil::StringView &value) {
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
//                       indexlib::index::KKVIterator* iter) __attribute__((always_inline));
// matchdoc::Reference<uint64_t>* GetTimestampRef() const { return _tsRef; }

void ValueCollector::setValue(MatchDoc doc, ReferenceBase *ref, const char *value) {
    assert(ref && value);
    matchdoc::ValueType valueType = ref->getValueType();
    assert(valueType.isBuiltInType() && !valueType.isMultiValue());
    matchdoc::BuiltinType fieldType = valueType.getBuiltinType();
    switch (fieldType) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        typedef matchdoc::BuiltinType2CppType<ft>::CppType T;                                      \
        matchdoc::Reference<T> *refTyped = static_cast<matchdoc::Reference<T> *>(ref);             \
        refTyped->set(doc, *(T *)value);                                                           \
        break;                                                                                     \
    }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        assert(false);
    }
    }
}

const string CollectorUtil::MOUNT_TABLE_GROUP = "__mount_table__";

MatchDocAllocatorPtr CollectorUtil::createMountedMatchDocAllocator(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
    const autil::mem_pool::PoolPtr &pool) {
    MountInfoPtr mountInfo = CollectorUtil::createPackAttributeMountInfo(schema);
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(pool, false, mountInfo));
    return allocator;
}

MountInfoPtr CollectorUtil::createPackAttributeMountInfo(
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    string tableName = schema->GetTableName();
    MountInfoPtr mountInfo(new MountInfo());
    uint32_t beginMountId = 0;
    auto legacySchemaAdapter
        = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
    if (legacySchemaAdapter) {
        CollectorUtil::insertPackAttributeMountInfo(
            mountInfo, legacySchemaAdapter->GetLegacySchema(), tableName, beginMountId);
    } else {
        CollectorUtil::insertPackAttributeMountInfoV2(mountInfo, schema, tableName, beginMountId);
    }
    return mountInfo;
}

void CollectorUtil::insertPackAttributeMountInfo(
    const MountInfoPtr &singleMountInfo,
    const indexlib::config::IndexPartitionSchemaPtr &schema,
    const string &tableName,
    uint32_t &beginMountId) {
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }

    indexlib::TableType tableType = schema->GetTableType();
    if (tableType == indexlib::tt_kv || tableType == indexlib::tt_kkv) {
        SingleFieldIndexConfigPtr pkIndexConfig
            = schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
        assert(pkIndexConfig);

        KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkIndexConfig);
        assert(kvIndexConfig);
        const ValueConfigPtr &valueConfig = kvIndexConfig->GetValueConfig();
        assert(valueConfig);
        PackAttributeConfigPtr packConf = valueConfig->CreatePackAttributeConfig();
        if (tableType == indexlib::tt_kv) {
            // TODO: kkv & kv should use same strategy later
            if (valueConfig->GetAttributeCount() == 1
                && !valueConfig->GetAttributeConfig(0)->IsMultiValue()
                && (valueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string)) {
                packConf.reset();
            }
        }
        insertPackAttributeMountInfo(singleMountInfo, packConf, tableName, beginMountId++);
        return;
    }

    for (size_t i = 0; i < attrSchema->GetPackAttributeCount(); i++) {
        const PackAttributeConfigPtr &packAttrConf
            = attrSchema->GetPackAttributeConfig((packattrid_t)i);
        assert(packAttrConf);
        insertPackAttributeMountInfo(singleMountInfo, packAttrConf, tableName, beginMountId++);
    }
}

void CollectorUtil::insertPackAttributeMountInfoV2(
    const MountInfoPtr &singleMountInfo,
    const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
    const string &tableName,
    uint32_t &beginMountId) {
    // indexlibv2 normal table does not support pack attribute yet
    if (schema->GetTableType() != indexlib::table::TABLE_TYPE_KV
        && schema->GetTableType() != indexlib::table::TABLE_TYPE_KKV
        && schema->GetTableType() != indexlib::table::TABLE_TYPE_AGGREGATION) {
        return;
    }

    auto pkConfig = schema->GetPrimaryKeyIndexConfig();
    assert(pkConfig);
    const auto &kvIndexConfig
        = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(pkConfig);
    assert(kvIndexConfig);

    const auto &valueConfig = kvIndexConfig->GetValueConfig();
    assert(valueConfig);
    auto [status, packConf] = valueConfig->CreatePackAttributeConfig();
    THROW_IF_STATUS_ERROR(status);
    if (valueConfig->GetAttributeCount() == 1 && !valueConfig->GetAttributeConfig(0)->IsMultiValue()
        && (valueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string)) {
        packConf.reset();
    }
    insertPackAttributeMountInfo(singleMountInfo, packConf, tableName, beginMountId++);
    // TODO: impl pack if indexlibv2 supportted
}

void CollectorUtil::insertPackAttributeMountInfo(
    const MountInfoPtr &singleMountInfo,
    const std::shared_ptr<indexlibv2::index::PackAttributeConfig> &packAttrConf,
    const string &tableName,
    uint32_t mountId) {
    if (!packAttrConf) {
        return;
    }
    vector<string> subAttrNames;
    packAttrConf->GetSubAttributeNames(subAttrNames);

    indexlibv2::index::PackAttributeFormatter packAttrFormatter;
    if (!packAttrFormatter.Init(packAttrConf)) {
        SQL_LOG(ERROR, "table [%s] init pack attribute formatter failed!", tableName.c_str());
        return;
    }

    for (size_t i = 0; i < subAttrNames.size(); i++) {
        auto attrRef = packAttrFormatter.GetAttributeReference(subAttrNames[i]);
        assert(attrRef);

        string fullName = tableName + ":" + subAttrNames[i];
        singleMountInfo->insert(fullName, mountId, attrRef->GetOffset().toUInt64());
        singleMountInfo->insert(subAttrNames[i], mountId, attrRef->GetOffset().toUInt64());
    }
}

#define FIELD_TYPE_MACRO_HELPER(MY_MACRO)                                                          \
    MY_MACRO(ft_int8);                                                                             \
    MY_MACRO(ft_int16);                                                                            \
    MY_MACRO(ft_int32);                                                                            \
    MY_MACRO(ft_int64);                                                                            \
    MY_MACRO(ft_uint8);                                                                            \
    MY_MACRO(ft_uint16);                                                                           \
    MY_MACRO(ft_uint32);                                                                           \
    MY_MACRO(ft_uint64);                                                                           \
    MY_MACRO(ft_float);                                                                            \
    MY_MACRO(ft_double);                                                                           \
    MY_MACRO(ft_string);                                                                           \
    MY_MACRO(ft_hash_64);

matchdoc::ReferenceBase *
CollectorUtil::declareReference(const matchdoc::MatchDocAllocatorPtr &allocator,
                                FieldType fieldType,
                                bool isMulti,
                                const string &valueName,
                                const string &groupName) {
    switch (fieldType) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        if (isMulti) {                                                                             \
            typedef indexlib::IndexlibFieldType2CppType<ft, true>::CppType T;                      \
            return allocator->declare<T>(valueName, groupName);                                    \
        } else {                                                                                   \
            typedef indexlib::IndexlibFieldType2CppType<ft, false>::CppType T;                     \
            return allocator->declare<T>(valueName, groupName);                                    \
        }                                                                                          \
    }
        FIELD_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    case ft_hash_128: {
        if (isMulti) {
            return NULL;
        } else {
            typedef indexlib::IndexlibFieldType2CppType<ft_hash_128, false>::CppType T;
            return allocator->declare<T>(valueName, groupName);
        }
    }
    default: {
        return NULL;
    }
    }
}

void CollectorUtil::moveToNextVaildDoc(indexlib::index::KKVIterator *iter,
                                       matchdoc::MatchDoc &matchDoc,
                                       const matchdoc::MatchDocAllocatorPtr &allocator,
                                       const ValueCollectorPtr &valueCollector,
                                       int &matchDocCount) {
    if (NULL == iter) {
        matchDoc = INVALID_MATCHDOC;
        return;
    }
    if (iter->IsValid()) {
        matchdoc::MatchDoc doc = allocator->allocate(matchDocCount++);
        valueCollector->collectFields(doc, iter);
        matchDoc = doc;
        iter->MoveToNext();
        return;
    }
    matchDoc = INVALID_MATCHDOC;
}

} // namespace sql
