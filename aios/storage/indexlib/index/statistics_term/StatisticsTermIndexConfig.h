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
#include "indexlib/config/IIndexConfig.h"

namespace indexlibv2::index {

class StatisticsTermIndexConfig : public config::IIndexConfig, public autil::legacy::Jsonizable
{
public:
    StatisticsTermIndexConfig();
    ~StatisticsTermIndexConfig();

public:
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    const std::string& GetIndexCommonPath() const override;
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<config::FieldConfig>> GetFieldConfigs() const override;
    void Check() const override;
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status CheckCompatible(const IIndexConfig* other) const override;
    bool IsDisabled() const override;

public:
    std::vector<std::string> GetInvertedIndexNames() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
