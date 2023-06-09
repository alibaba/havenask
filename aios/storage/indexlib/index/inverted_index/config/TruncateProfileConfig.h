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
#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/index/inverted_index/config/PayloadConfig.h"

namespace indexlibv2::config {

class TruncateProfileConfig : public autil::legacy::Jsonizable
{
public:
    TruncateProfileConfig();
    TruncateProfileConfig(const TruncateProfileConfig& other);
    virtual ~TruncateProfileConfig();
    TruncateProfileConfig& operator=(const TruncateProfileConfig& other);

public:
    const std::string& GetTruncateProfileName() const;
    const std::vector<indexlib::config::SortParam>& GetTruncateSortParams() const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    const std::map<std::string, std::string>& GetDocPayloadParams() const;
    const std::string& GetPayloadName() const;
    const indexlib::config::PayloadConfig& GetPayloadConfig() const;
    void Check() const;

    Status CheckEqual(const TruncateProfileConfig& other) const;

public:
    static bool IsSortParamByDocPayload(const indexlib::config::SortParam& sortParam);

public:
    void TEST_SetPayloadName(const std::string& payloadName);

protected:
    void TEST_SetTruncateProfileName(const std::string& profileName);
    void TEST_SetSortParams(const std::string& sortDesp);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
