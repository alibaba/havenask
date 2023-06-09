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
#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"

#include "TruncateProfileConfig.h"
#include "indexlib/config/ConfigDefine.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, TruncateProfileConfig);

struct TruncateProfileConfig::Impl {
    std::string truncateProfileName;
    std::string oriSortDescriptions;
    std::vector<indexlib::config::SortParam> sortParams;
    std::map<std::string, std::string> docPayloadParams;
    indexlib::config::PayloadConfig payloadConfig;
};

TruncateProfileConfig::TruncateProfileConfig() : _impl(std::make_unique<TruncateProfileConfig::Impl>()) {}
TruncateProfileConfig::TruncateProfileConfig(const TruncateProfileConfig& other)
    : _impl(std::make_unique<TruncateProfileConfig::Impl>(*other._impl))
{
}
TruncateProfileConfig& TruncateProfileConfig::operator=(const TruncateProfileConfig& other)
{
    if (this != &other) {
        _impl = std::make_unique<TruncateProfileConfig::Impl>(*other._impl);
    }
    return *this;
}
TruncateProfileConfig::~TruncateProfileConfig() {}
void TruncateProfileConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("truncate_profile_name", _impl->truncateProfileName, _impl->truncateProfileName);
    json.Jsonize("docpayload_parameters", _impl->docPayloadParams, _impl->docPayloadParams);
    if (json.GetMode() == TO_JSON) {
        if (!_impl->sortParams.empty()) {
            std::string sortDespStr = SortParamsToString(_impl->sortParams);
            json.Jsonize("sort_descriptions", sortDespStr);
        }
        if (_impl->payloadConfig.IsInitialized()) {
            json.Jsonize("payload_name", _impl->payloadConfig.GetName());
        }

    } else {
        json.Jsonize("sort_descriptions", _impl->oriSortDescriptions, _impl->oriSortDescriptions);
        _impl->sortParams = indexlib::config::StringToSortParams(_impl->oriSortDescriptions);
        std::string payloadName;
        json.Jsonize("payload_name", payloadName, payloadName);
        if (!payloadName.empty()) {
            _impl->payloadConfig.SetName(payloadName);
            auto iter = _impl->docPayloadParams.find("DOC_PAYLOAD_USE_FP_16");
            if (iter != _impl->docPayloadParams.end() && iter->second == "true") {
                _impl->payloadConfig.SetFP16(true);
            } else {
                _impl->payloadConfig.SetFP16(false);
            }
        }
    }
}

const std::string& TruncateProfileConfig::GetTruncateProfileName() const { return _impl->truncateProfileName; }

const std::vector<indexlib::config::SortParam>& TruncateProfileConfig::GetTruncateSortParams() const
{
    return _impl->sortParams;
}
const std::string& TruncateProfileConfig::GetPayloadName() const { return GetPayloadConfig().GetName(); }

const indexlib::config::PayloadConfig& TruncateProfileConfig::GetPayloadConfig() const { return _impl->payloadConfig; }
void TruncateProfileConfig::Check() const
{
    if (_impl->truncateProfileName.empty()) {
        AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "truncate profile name is empty");
    }
    const bool isPayloadInitialized = _impl->payloadConfig.IsInitialized();
    bool containsDocPayload = false;
    for (const indexlib::config::SortParam& sp : _impl->sortParams) {
        if (TruncateProfileConfig::IsSortParamByDocPayload(sp)) {
            containsDocPayload = true;
        }
    }
    if (isPayloadInitialized && !containsDocPayload) {
        std::string error = "Payload configured but DOC_PAYLOAD is not used in sort param, name:";
        error += _impl->payloadConfig.GetName().c_str();
        AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, error.c_str());
    }
}

Status TruncateProfileConfig::CheckEqual(const TruncateProfileConfig& other) const
{
    CHECK_CONFIG_EQUAL(_impl->truncateProfileName, other._impl->truncateProfileName,
                       "TruncateProfileName doesn't match");

    CHECK_CONFIG_EQUAL(_impl->sortParams.size(), other._impl->sortParams.size(), "SortParams's size not equal");
    for (size_t i = 0; i < _impl->sortParams.size(); ++i) {
        _impl->sortParams[i].AssertEqual(other._impl->sortParams[i]);
    }

    CHECK_CONFIG_EQUAL(_impl->docPayloadParams.size(), other._impl->docPayloadParams.size(),
                       "DocPayloadParams's size not equal");
    for (const auto& kv : _impl->docPayloadParams) {
        auto iter = other._impl->docPayloadParams.find(kv.first);
        if (iter == other._impl->docPayloadParams.end()) {
            AUTIL_LOG(ERROR, "[%s] not found in otehr truncate profile docpayload_parameters", kv.first.c_str());
            return Status::ConfigError();
        }
        auto errorMsg = kv.first + "'s value is not equal";
        CHECK_CONFIG_EQUAL(kv.second, iter->second, errorMsg.c_str());
    }

    CHECK_CONFIG_EQUAL(_impl->payloadConfig.GetName(), other._impl->payloadConfig.GetName(),
                       "payloadConfig's Name doesn't match");
    return Status::OK();
}

void TruncateProfileConfig::TEST_SetPayloadName(const std::string& payloadName)
{
    _impl->payloadConfig.SetName(payloadName);
}

const std::map<std::string, std::string>& TruncateProfileConfig::GetDocPayloadParams() const
{
    return _impl->docPayloadParams;
}

bool TruncateProfileConfig::IsSortParamByDocPayload(const indexlib::config::SortParam& sortParam)
{
    return sortParam.GetSortField() == "DOC_PAYLOAD";
}

void TruncateProfileConfig::TEST_SetTruncateProfileName(const std::string& profileName)
{
    _impl->truncateProfileName = profileName;
}

void TruncateProfileConfig::TEST_SetSortParams(const std::string& sortDesp)
{
    _impl->oriSortDescriptions = sortDesp;
    _impl->sortParams = indexlib::config::StringToSortParams(sortDesp);
}

} // namespace indexlibv2::config
