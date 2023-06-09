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
#include "indexlib/index/kkv/config/KKVIndexFieldInfo.h"

#include "indexlib/config/ConfigDefine.h"

using namespace std;

namespace indexlib::config {

AUTIL_LOG_SETUP(indexlib.config, KKVIndexFieldInfo);

KKVIndexFieldInfo::KKVIndexFieldInfo()
    : keyType(KKVKeyType::PREFIX)
    , countLimits(INVALID_COUNT_LIMITS)
    , skipListThreshold(INVALID_SKIPLIST_THRESHOLD)
    , protectionThreshold(DEFAULT_PROTECTION_THRESHOLD)
    , enableStoreOptimize(true)
    , enableKeepSortSequence(false)
{
}

void KKVIndexFieldInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("field_name", fieldName);
    if (json.GetMode() == TO_JSON) {
        string keyTypeString = KKVKeyTypeToString(keyType);
        json.Jsonize("key_type", keyTypeString);
        if (countLimits != INVALID_COUNT_LIMITS) {
            json.Jsonize("count_limits", countLimits);
        }
        if (skipListThreshold != INVALID_SKIPLIST_THRESHOLD) {
            json.Jsonize("skip_list_threshold", skipListThreshold);
        }
        if (!sortParams.empty()) {
            string sortParamDesp = SortParamsToString(sortParams);
            json.Jsonize("trunc_sort_params", sortParamDesp);
        }
        if (!enableStoreOptimize) {
            json.Jsonize("enable_store_optimize", enableStoreOptimize);
        }
        if (protectionThreshold != DEFAULT_PROTECTION_THRESHOLD) {
            json.Jsonize(KKV_BUILD_PROTECTION_THRESHOLD, protectionThreshold);
        }
        if (enableKeepSortSequence) {
            json.Jsonize("enable_keep_sort_sequence", enableKeepSortSequence);
        }
    } else {
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

indexlibv2::Status KKVIndexFieldInfo::CheckEqual(const KKVIndexFieldInfo& other) const
{
    CHECK_CONFIG_EQUAL(fieldName, other.fieldName, "fieldName not equal");
    CHECK_CONFIG_EQUAL(keyType, other.keyType, "keyType not equal");
    CHECK_CONFIG_EQUAL(countLimits, other.countLimits, "countLimits not equal");
    CHECK_CONFIG_EQUAL(skipListThreshold, other.skipListThreshold, "skipListThreshold not equal");
    CHECK_CONFIG_EQUAL(protectionThreshold, other.protectionThreshold, "protectionThreshold not equal");
    CHECK_CONFIG_EQUAL(enableStoreOptimize, other.enableStoreOptimize, "enableStoreOptimize not equal");
    CHECK_CONFIG_EQUAL(enableKeepSortSequence, other.enableKeepSortSequence, "enableKeepSortSequence not equal");
    return Status::OK();
}

string KKVIndexFieldInfo::KKVKeyTypeToString(KKVKeyType keyType)
{
    switch (keyType) {
    case KKVKeyType::PREFIX:
        return "prefix";
    case KKVKeyType::SUFFIX:
        return "suffix";
    }
    assert(false);
    return "invalid_kkv_key_string";
}

KKVKeyType KKVIndexFieldInfo::StringToKKVKeyType(const string& str)
{
    if (str == "prefix") {
        return KKVKeyType::PREFIX;
    }
    if (str == "suffix") {
        return KKVKeyType::SUFFIX;
    }
    assert(false);
    return KKVKeyType::PREFIX;
}
} // namespace indexlib::config
