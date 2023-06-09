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
#pragma once

#include "autil/Log.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::config {
class FieldConfig;
}

namespace indexlib::config {

class SummaryConfig
{
public:
    SummaryConfig();
    ~SummaryConfig();

public:
    FieldType GetFieldType() const;
    void SetFieldConfig(const std::shared_ptr<indexlibv2::config::FieldConfig>& fieldConfig);
    std::shared_ptr<indexlibv2::config::FieldConfig> GetFieldConfig() const;
    fieldid_t GetFieldId() const;
    const std::string& GetSummaryName() const;
    Status CheckEqual(const SummaryConfig& other) const;
    bool SupportNull() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
