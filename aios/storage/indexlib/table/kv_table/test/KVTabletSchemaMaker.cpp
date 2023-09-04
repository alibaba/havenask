#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"

#include "autil/StringTokenizer.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/config/test/FieldConfigMaker.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"

using namespace std;
using namespace indexlibv2::config;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTabletSchemaMaker);

std::shared_ptr<config::TabletSchema>
KVTabletSchemaMaker::Make(const std::string& fieldNames, const std::string& keyName, const std::string& valueNames,
                          int64_t ttl, const std::string& valueFormat, bool needStorePKValue, bool useNumberHash)
{
    auto schema = std::make_shared<TabletSchema>();
    auto unresolvedSchema = schema->TEST_GetImpl();
    unresolvedSchema->TEST_SetTableName("noname");
    unresolvedSchema->TEST_SetTableType("kv");
    if (fieldNames.size() > 0 && !MakeFieldConfig(unresolvedSchema, fieldNames).IsOK()) {
        AUTIL_LOG(ERROR, "make field config failed");
        return nullptr;
    }
    if (!MakeKVIndexConfig(unresolvedSchema, keyName, valueNames, ttl, valueFormat, useNumberHash).IsOK()) {
        AUTIL_LOG(ERROR, "make kv index config failed");
        return nullptr;
    }
    if (needStorePKValue && !unresolvedSchema->TEST_SetSetting(index::NEED_STORE_PK_VALUE, true, false)) {
        AUTIL_LOG(ERROR, "set NEED_STORE_PK_VALUE failed");
        return nullptr;
    }
    return schema;
}

Status KVTabletSchemaMaker::MakeFieldConfig(UnresolvedSchema* schema, const string& fieldNames)
{
    // fieldName:fieldType:isMultiValue:FixedMultiValueCount:EnableNullField:AnalyzerName
    auto fieldConfigs = FieldConfigMaker::Make(fieldNames);
    for (const auto& fieldConfig : fieldConfigs) {
        RETURN_IF_STATUS_ERROR(schema->AddFieldConfig(fieldConfig), "add field config [%s] failed",
                               fieldConfig->GetFieldName().c_str());
    }
    return Status::OK();
}

Status KVTabletSchemaMaker::MakeKVIndexConfig(config::UnresolvedSchema* schema, const std::string& keyName,
                                              const std::string& valueNames, int64_t ttl,
                                              const std::string& valueFormat, bool useNumberHash)
{
    auto kvIndexConfig = make_shared<KVIndexConfig>();
    kvIndexConfig->SetIndexName(keyName);
    auto keyFieldConfig = schema->GetFieldConfig(keyName);
    if (!keyFieldConfig) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "key field [%s] not exist in schema", keyName.c_str());
    }
    kvIndexConfig->SetFieldConfig(keyFieldConfig);
    if (ttl != indexlib::INVALID_TTL) {
        kvIndexConfig->SetTTL(ttl);
    }
    auto status = schema->DoAddIndexConfig(kvIndexConfig->GetIndexType(), kvIndexConfig, true);
    RETURN_IF_STATUS_ERROR(status, "add kv index config failed");
    kvIndexConfig->SetUseNumberHash(useNumberHash);
    auto preference = kvIndexConfig->GetIndexPreference();
    auto valueParam = preference.GetValueParam();
    if (valueFormat == "plain") {
        valueParam.EnablePlainFormat(true);
    } else if (valueFormat == "impact") {
        valueParam.EnableValueImpact(true);
    } else if (valueFormat == "autoInline") {
        valueParam.EnableFixLenAutoInline();
    } else {
        valueParam.EnablePlainFormat(false);
        valueParam.EnableValueImpact(false);
    }
    preference.SetValueParam(valueParam);
    kvIndexConfig->SetIndexPreference(preference);
    auto valueNameVec = FieldConfigMaker::SplitToStringVector(valueNames);
    vector<shared_ptr<index::AttributeConfig>> attrConfigs;
    size_t attrId = 0;
    for (const auto& valueName : valueNameVec) {
        // name:updatable:compressStr
        autil::StringTokenizer st(valueName, ":", autil::StringTokenizer::TOKEN_TRIM);
        if (st.getNumTokens() <= 0) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "invalid value names [%s]", valueNames.c_str());
        }
        const auto& fieldName = st[0];
        const auto& fieldConfig = schema->GetFieldConfig(fieldName);
        if (!fieldConfig) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "get value field [%s] failed", fieldName.c_str());
        }
        auto attrConfig = make_shared<index::AttributeConfig>();
        auto status = attrConfig->Init(fieldConfig);
        RETURN_IF_STATUS_ERROR(status, "attr config init failed, field [%s]", fieldName.c_str());

        attrConfig->SetAttrId(attrId++);
        if (st.getNumTokens() >= 2 && st[1] == "true") {
            attrConfig->SetUpdatable(true);
        }
        if (st.getNumTokens() >= 3) {
            std::string compressStr = st[2];
            RETURN_IF_STATUS_ERROR(attrConfig->SetCompressType(compressStr), "set compress [%s] failed",
                                   compressStr.c_str());
        }
        attrConfigs.push_back(attrConfig);
    }

    auto valueConfig = make_shared<config::ValueConfig>();
    RETURN_IF_STATUS_ERROR(valueConfig->Init(attrConfigs), "value config init failed");
    valueConfig->EnableValueImpact(kvIndexConfig->GetIndexPreference().GetValueParam().IsValueImpact());
    valueConfig->EnablePlainFormat(kvIndexConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
    valueConfig->EnableCompactFormat(true);
    kvIndexConfig->SetValueConfig(valueConfig);
    return Status::OK();
}

} // namespace indexlibv2::table
