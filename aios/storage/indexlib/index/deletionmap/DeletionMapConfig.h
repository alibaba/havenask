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

#include "indexlib/config/IIndexConfig.h"

namespace indexlibv2::index {
class DeletionMapConfig final : public config::IIndexConfig
{
public:
    DeletionMapConfig() {}
    ~DeletionMapConfig() {}

    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    void Check() const override { assert(false); }
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override
    {
    }
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override {}
    Status CheckCompatible(const IIndexConfig* other) const override { return Status::OK(); }
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<config::FieldConfig>> GetFieldConfigs() const override
    {
        // 去掉 assert false，因为 AddToUpdateDocumentRewriter 检查的时候会遍历到
        return {};
    }
};

} // namespace indexlibv2::index
