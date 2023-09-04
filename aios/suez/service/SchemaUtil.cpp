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
#include "suez/service/SchemaUtil.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/index/common/Constant.h"

using namespace std;
using namespace indexlib;
using namespace indexlib::config;

namespace suez {

static ErrorInfo makeErrorInfo(ErrorCode code, const std::string &msg = "") {
    ErrorInfo errInfo;
    errInfo.set_errorcode(code);
    errInfo.set_errormsg(msg);
    return errInfo;
}

ErrorInfo SchemaUtil::getPbSchema(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                  const string &targetAttr,
                                  ResultSchema &resultSchema) {
    const auto &legacySchemaAdapter = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
    // TODO: when indexlibv2 support pack attribute, remove this logic
    if (legacySchemaAdapter) {
        return getPbSchema(legacySchemaAdapter->GetLegacySchema(), targetAttr, resultSchema);
    }
    string tableTypeStr = schema->GetTableType();
    resultSchema.set_tabletype(tableTypeStr);
    string schemaName = schema->GetTableName();
    resultSchema.set_schemaname(schemaName);
    const auto &fieldConfigs = schema->GetIndexFieldConfigs(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR);
    for (const auto &fieldConfig : fieldConfigs) {
        string attrName = fieldConfig->GetFieldName();
        if (targetAttr.empty() || attrName == targetAttr) {
            AttributeResultSchema attrResultSchema;
            attrResultSchema.set_attrname(attrName);
            ValueType valueType;
            ErrorInfo errorInfo = convertFieldType(fieldConfig->GetFieldType(), valueType);
            if (errorInfo.errorcode() == TBS_ERROR_NONE) {
                attrResultSchema.set_valuetype(valueType);
            } else {
                return errorInfo;
            }
            attrResultSchema.set_ismulti(fieldConfig->IsMultiValue());
            *(resultSchema.add_attrschema()) = attrResultSchema;
        }
    }
    return ErrorInfo::default_instance();
}

ErrorInfo SchemaUtil::getPbSchema(const IndexPartitionSchemaPtr &indexPartitionSchemaPtr,
                                  const string &targetAttr,
                                  ResultSchema &resultSchema) {
    string tableTypeStr;
    tableTypeToStr(indexPartitionSchemaPtr->GetTableType(), tableTypeStr);
    resultSchema.set_tabletype(tableTypeStr);
    string schemaName = indexPartitionSchemaPtr->GetSchemaName();
    resultSchema.set_schemaname(schemaName);
    const auto &attrSchema = indexPartitionSchemaPtr->GetAttributeSchema();
    if (nullptr == attrSchema) {
        return makeErrorInfo(TBS_ERROR_NONE, "failed to get attribute schema");
    }
    const auto &attrIter = attrSchema->CreateIterator();
    if (nullptr == attrIter) {
        return makeErrorInfo(TBS_ERROR_GET_SCHEMA, "failed to get attribute schema iterator");
    }

    for (auto attrConfigIter = attrIter->Begin(); attrConfigIter != attrIter->End(); ++attrConfigIter) {
        string attrName = (*attrConfigIter)->GetAttrName();
        if (targetAttr.empty() || attrName == targetAttr) {
            AttributeResultSchema attrResultSchema;
            attrResultSchema.set_attrname(attrName);
            ValueType valueType;
            ErrorInfo errorInfo = convertFieldType((*attrConfigIter)->GetFieldType(), valueType);
            if (errorInfo.errorcode() == TBS_ERROR_NONE) {
                attrResultSchema.set_valuetype(valueType);
            } else {
                return errorInfo;
            }
            attrResultSchema.set_ismulti((*attrConfigIter)->IsMultiValue());
            *(resultSchema.add_attrschema()) = attrResultSchema;
        }
    }
    return ErrorInfo::default_instance();
}

ErrorInfo SchemaUtil::getJsonSchema(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                    ResultSchema &resultSchema) {
    string jsonSchema;
    schema->Serialize(/*isCompact*/ false, &jsonSchema);
    resultSchema.set_jsonschema(jsonSchema);
    return ErrorInfo::default_instance();
}

ErrorInfo SchemaUtil::convertFieldType(const FieldType &fieldType, ValueType &valueType) {
    switch (fieldType) {
    case ft_int8:
        valueType = ValueType::INT_8;
        break;
    case ft_uint8:
        valueType = ValueType::UINT_8;
        break;
    case ft_int16:
        valueType = ValueType::INT_16;
        break;
    case ft_uint16:
        valueType = ValueType::UINT_16;
        break;
    case ft_int32:
        valueType = ValueType::INT_32;
        break;
    case ft_uint32:
        valueType = ValueType::UINT_32;
        break;
    case ft_int64:
        valueType = ValueType::INT_64;
        break;
    case ft_uint64:
        valueType = ValueType::UINT_64;
        break;
    case ft_float:
        valueType = ValueType::FLOAT;
        break;
    case ft_double:
        valueType = ValueType::DOUBLE;
        break;
    case ft_string:
        valueType = ValueType::STRING;
        break;
    default:
        return makeErrorInfo(TBS_ERROR_NOT_SUPPORTED_VALUE_TYPE, "current only support int, float, double and string.");
    }
    return ErrorInfo::default_instance();
}

void SchemaUtil::tableTypeToStr(const TableType &tableType, std::string &tableTypeStr) {
    switch (tableType) {
    case tt_index:
        tableTypeStr = TABLE_TYPE_INDEX;
        break;
    case tt_kv:
        tableTypeStr = TABLE_TYPE_KV;
        break;
    case tt_kkv:
        tableTypeStr = TABLE_TYPE_KKV;
        break;
    case tt_rawfile:
        tableTypeStr = TABLE_TYPE_RAWFILE;
        break;
    case tt_linedata:
        tableTypeStr = TABLE_TYPE_LINEDATA;
        break;
    case tt_customized:
        tableTypeStr = TABLE_TYPE_CUSTOMIZED;
        break;
    default:
        break;
    }
}

} // namespace suez
