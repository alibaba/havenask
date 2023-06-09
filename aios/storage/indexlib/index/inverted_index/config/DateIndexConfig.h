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

#include "indexlib/index/inverted_index/config/DateLevelFormat.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"

namespace indexlibv2::config {

class DateIndexConfig : public SingleFieldIndexConfig
{
public:
    DateIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    DateIndexConfig(const std::string& indexName, InvertedIndexType indexType, const std::string& defaultGranularity);
    DateIndexConfig(const DateIndexConfig& other);
    ~DateIndexConfig();

public:
    typedef indexlib::config::DateLevelFormat::Granularity Granularity;

public:
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    Status CheckEqual(const InvertedIndexConfig& other) const override;
    void Check() const override;
    InvertedIndexConfig* Clone() const override;
    Granularity GetBuildGranularity() const;
    indexlib::config::DateLevelFormat GetDateLevelFormat() const;

public:
    void SetDefaultGranularity(const std::string& defaultGranularity);
    void SetBuildGranularity(Granularity granularity);
    void SetDateLevelFormat(indexlib::config::DateLevelFormat format);

protected:
    void DoDeserialize(const autil::legacy::Any& any, const config::IndexConfigDeserializeResource& resource) override;

private:
    std::string GranularityToString(Granularity granularity) const;
    Granularity StringToGranularity(const std::string& str);

private:
    inline static const std::string BUILD_GRANULARITY = "build_granularity";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
