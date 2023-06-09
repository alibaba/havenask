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
#include "indexlib/index/summary/config/SummaryConfig.h"

#include "indexlib/config/FieldConfig.h"

namespace indexlib::config {
AUTIL_LOG_SETUP(indexlib.config, SummaryConfig);

struct SummaryConfig::Impl {
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig;
};

SummaryConfig::SummaryConfig() : _impl(std::make_unique<Impl>()) {}

SummaryConfig::~SummaryConfig() {}

FieldType SummaryConfig::GetFieldType() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->GetFieldType();
}

void SummaryConfig::SetFieldConfig(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig)
{
    assert(fieldConfig);
    _impl->fieldConfig = fieldConfig;
}

std::shared_ptr<indexlibv2::config::FieldConfig> SummaryConfig::GetFieldConfig() const { return _impl->fieldConfig; }

fieldid_t SummaryConfig::GetFieldId() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->GetFieldId();
}

const std::string& SummaryConfig::GetSummaryName() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->GetFieldName();
}

Status SummaryConfig::CheckEqual(const SummaryConfig& other) const
{
    if (_impl->fieldConfig && other._impl->fieldConfig) {
        return _impl->fieldConfig->CheckEqual(*(other._impl->fieldConfig));
    } else if (!_impl->fieldConfig || other._impl->fieldConfig) {
        AUTIL_LOG(ERROR, "summary config is not equal, [%p] [%p]", _impl->fieldConfig.get(),
                  other._impl->fieldConfig.get());
        return Status::ConfigError();
    }
    return Status::OK();
}

bool SummaryConfig::SupportNull() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->IsEnableNullField();
}

} // namespace indexlib::config
