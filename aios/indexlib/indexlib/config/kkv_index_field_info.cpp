#include "indexlib/config/kkv_index_field_info.h"
#include "indexlib/config/configurator_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);

IE_LOG_SETUP(config, KKVIndexFieldInfo);

const uint32_t KKVIndexFieldInfo::INVALID_COUNT_LIMITS = numeric_limits<uint32_t>::max();
const uint32_t KKVIndexFieldInfo::INVALID_SKIPLIST_THRESHOLD = numeric_limits<uint32_t>::max();
const uint32_t KKVIndexFieldInfo::DEFAULT_PROTECTION_THRESHOLD = numeric_limits<uint32_t>::max();

KKVIndexFieldInfo::KKVIndexFieldInfo()
    : keyType(kkv_prefix)
    , countLimits(INVALID_COUNT_LIMITS)
    , skipListThreshold(INVALID_SKIPLIST_THRESHOLD)
    , protectionThreshold(DEFAULT_PROTECTION_THRESHOLD)
    , enableStoreOptimize(true)
    , enableKeepSortSequence(false)
{}

void KKVIndexFieldInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("field_name", fieldName);
    if (json.GetMode() == TO_JSON)
    {
        string keyTypeString = KKVKeyTypeToString(keyType);
        json.Jsonize("key_type", keyTypeString);
        if (countLimits != INVALID_COUNT_LIMITS)
        {
            json.Jsonize("count_limits", countLimits);
        }
        if (skipListThreshold != INVALID_SKIPLIST_THRESHOLD)
        {
            json.Jsonize("skip_list_threshold", skipListThreshold);
        }
        if (!sortParams.empty())
        {
            string sortParamDesp = SortParamsToString(sortParams);
            json.Jsonize("trunc_sort_params", sortParamDesp);
        }
        if (!enableStoreOptimize)
        {
            json.Jsonize("enable_store_optimize", enableStoreOptimize);
        }
        if (protectionThreshold != DEFAULT_PROTECTION_THRESHOLD)
        {
            json.Jsonize(KKV_BUILD_PROTECTION_THRESHOLD, protectionThreshold);
        }
        if (enableKeepSortSequence)
        {
            json.Jsonize("enable_keep_sort_sequence", enableKeepSortSequence);
        }
    }
    else
    {
        string keyTypeString;
        json.Jsonize("key_type", keyTypeString);
        keyType = StringToKKVKeyType(keyTypeString);
        json.Jsonize("count_limits", countLimits, INVALID_COUNT_LIMITS);
        json.Jsonize("skip_list_threshold", skipListThreshold, INVALID_SKIPLIST_THRESHOLD);
        string sortParamStr;
        json.Jsonize("trunc_sort_params", sortParamStr, sortParamStr);
        sortParams = StringToSortParams(sortParamStr);
        json.Jsonize("enable_store_optimize", enableStoreOptimize, enableStoreOptimize);
        json.Jsonize(KKV_BUILD_PROTECTION_THRESHOLD, protectionThreshold, protectionThreshold);
        json.Jsonize("enable_keep_sort_sequence", enableKeepSortSequence, enableKeepSortSequence);
    }
}

void KKVIndexFieldInfo::AssertEqual(const KKVIndexFieldInfo& other) const
{
    IE_CONFIG_ASSERT_EQUAL(fieldName, other.fieldName, "fieldName not equal");
    IE_CONFIG_ASSERT_EQUAL(keyType, other.keyType, "keyType not equal");
    IE_CONFIG_ASSERT_EQUAL(countLimits, other.countLimits, "countLimits not equal");
    IE_CONFIG_ASSERT_EQUAL(skipListThreshold, other.skipListThreshold, "skipListThreshold not equal");
    IE_CONFIG_ASSERT_EQUAL(protectionThreshold, other.protectionThreshold,
                           "protectionThreshold not equal");
    IE_CONFIG_ASSERT_EQUAL(enableStoreOptimize, other.enableStoreOptimize,
                           "enableStoreOptimize not equal");
    IE_CONFIG_ASSERT_EQUAL(enableKeepSortSequence, other.enableKeepSortSequence,
                           "enableKeepSortSequence not equal");
}
    
string KKVIndexFieldInfo::KKVKeyTypeToString(KKVKeyType keyType)
{
    switch (keyType)
    {
    case kkv_prefix:
        return "prefix";
    case kkv_suffix:
        return "suffix";
    }
    assert(false);
    return "invalid_kkv_key_string";
}

KKVKeyType KKVIndexFieldInfo::StringToKKVKeyType(const string& str)
{
    if (str == "prefix")
    {
        return kkv_prefix;
    }
    if (str == "suffix")
    {
        return kkv_suffix;
    }
    assert(false);
    return kkv_prefix;
}


IE_NAMESPACE_END(config);

