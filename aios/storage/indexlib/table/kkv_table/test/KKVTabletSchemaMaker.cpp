#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/config/test/FieldConfigMaker.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kv/Common.h"

using namespace std;
using namespace indexlibv2::config;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVTabletSchemaMaker);

std::shared_ptr<config::TabletSchema> KKVTabletSchemaMaker::Make(const std::string& fieldNames,
                                                                 const std::string& pkeyName,
                                                                 const std::string& skeyName,
                                                                 const std::string& valueNames, int64_t ttl,
                                                                 const std::string& valueFormat, bool needStorePKValue)
{
    auto schema = std::make_shared<TabletSchema>();
    auto unresolvedSchema = schema->TEST_GetImpl();
    unresolvedSchema->TEST_SetTableName("noname");
    unresolvedSchema->TEST_SetTableType("kkv");
    if (fieldNames.size() > 0 && !MakeFieldConfig(unresolvedSchema, fieldNames).IsOK()) {
        AUTIL_LOG(ERROR, "make field config failed");
        return nullptr;
    }
    if (!MakeKKVIndexConfig(unresolvedSchema, pkeyName, skeyName, valueNames, ttl, valueFormat).IsOK()) {
        AUTIL_LOG(ERROR, "make kv index config failed");
        return nullptr;
    }
    if (needStorePKValue && !unresolvedSchema->TEST_SetSetting(index::NEED_STORE_PK_VALUE, true, false)) {
        AUTIL_LOG(ERROR, "set NEED_STORE_PK_VALUE failed");
        return nullptr;
    }
    return schema;
}

Status KKVTabletSchemaMaker::MakeFieldConfig(UnresolvedSchema* schema, const string& fieldNames)
{
    // fieldName:fieldType:isMultiValue:FixedMultiValueCount:EnableNullField:AnalyzerName
    const auto& fieldConfigs = FieldConfigMaker::Make(fieldNames);
    for (const auto& fieldConfig : fieldConfigs) {
        RETURN_IF_STATUS_ERROR(schema->AddFieldConfig(fieldConfig), "add field config [%s] failed",
                               fieldConfig->GetFieldName().c_str());
    }
    return Status::OK();
}

Status KKVTabletSchemaMaker::MakeKKVIndexConfig(config::UnresolvedSchema* schema, const std::string& pkeyName,
                                                const std::string& skeyName, const std::string& valueNames, int64_t ttl,
                                                const std::string& valueFormat)
{
    auto kkvIndexConfig = make_shared<KKVIndexConfig>();
    kkvIndexConfig->SetIndexName(pkeyName);
    auto pkeyFieldConfig = schema->GetFieldConfig(pkeyName);
    if (!pkeyFieldConfig) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "pkey field [%s] not exist in schema", pkeyName.c_str());
    }
    kkvIndexConfig->SetPrefixFieldConfig(pkeyFieldConfig);
    auto skeyFieldConfig = schema->GetFieldConfig(skeyName);
    if (!skeyFieldConfig) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "skey field [%s] not exist in schema", skeyName.c_str());
    }
    kkvIndexConfig->SetSuffixFieldConfig(skeyFieldConfig);
    if (ttl != indexlib::INVALID_TTL) {
        kkvIndexConfig->SetTTL(ttl);
    }
    indexlib::config::KKVIndexFieldInfo pkeyFieldInfo;
    pkeyFieldInfo.fieldName = pkeyName;
    pkeyFieldInfo.keyType = indexlib::config::KKVKeyType::PREFIX;
    kkvIndexConfig->SetPrefixFieldInfo(pkeyFieldInfo);
    indexlib::config::KKVIndexFieldInfo skeyFieldInfo;
    skeyFieldInfo.fieldName = skeyName;
    skeyFieldInfo.keyType = indexlib::config::KKVKeyType::SUFFIX;
    uint32_t skipListThreshold = autil::EnvUtil::getEnv("indexlib_kkv_default_skiplist_threshold",
                                                        indexlibv2::index::KKV_DEFAULT_SKIPLIST_THRESHOLD);
    skeyFieldInfo.skipListThreshold = skipListThreshold;
    kkvIndexConfig->SetSuffixFieldInfo(skeyFieldInfo);
    auto status = schema->DoAddIndexConfig(kkvIndexConfig->GetIndexType(), kkvIndexConfig, true);
    RETURN_IF_STATUS_ERROR(status, "add kv index config failed");
    auto preference = kkvIndexConfig->GetIndexPreference();
    auto valueParam = preference.GetValueParam();
    if (valueFormat == "plain") {
        valueParam.EnablePlainFormat(true);
    } else if (valueFormat == "impact") {
        valueParam.EnableValueImpact(true);
    } else {
        valueParam.EnablePlainFormat(false);
        valueParam.EnableValueImpact(false);
    }
    preference.SetValueParam(valueParam);
    kkvIndexConfig->SetIndexPreference(preference);
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
    if (kkvIndexConfig->GetSuffixFieldInfo().enableStoreOptimize) {
        OptimizeKKVSKeyStore(kkvIndexConfig, attrConfigs);
    }

    auto valueConfig = make_shared<config::ValueConfig>();
    RETURN_IF_STATUS_ERROR(valueConfig->Init(attrConfigs), "value config init failed");
    valueConfig->EnableValueImpact(kkvIndexConfig->GetIndexPreference().GetValueParam().IsValueImpact());
    valueConfig->EnablePlainFormat(kkvIndexConfig->GetIndexPreference().GetValueParam().IsPlainFormat());
    valueConfig->EnableCompactFormat(true);
    kkvIndexConfig->SetValueConfig(valueConfig);
    return Status::OK();
}

void KKVTabletSchemaMaker::OptimizeKKVSKeyStore(const shared_ptr<KKVIndexConfig>& kkvIndexConfig,
                                                vector<shared_ptr<index::AttributeConfig>>& attrConfigs)
{
    if (kkvIndexConfig->GetSuffixHashFunctionType() == indexlib::hft_murmur) {
        return;
    }
    const auto& skeyFieldConfig = kkvIndexConfig->GetSuffixFieldConfig();
    vector<shared_ptr<index::AttributeConfig>> optAttrConfigs;
    for (size_t i = 0; i < attrConfigs.size(); i++) {
        if (attrConfigs[i]->GetAttrName() == skeyFieldConfig->GetFieldName()) {
            continue;
        }
        optAttrConfigs.push_back(attrConfigs[i]);
    }
    // TODO: ValueConfig support empty attribute configs
    if (optAttrConfigs.empty()) {
        return;
    }
    attrConfigs.swap(optAttrConfigs);
    kkvIndexConfig->SetOptimizedStoreSKey(true);
}

} // namespace indexlibv2::table
