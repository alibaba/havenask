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
    static constexpr uint32_t DEFAULT_BITMAP_SIZE = 8 * 1024 * 1024;

public:
    DeletionMapConfig() {}
    ~DeletionMapConfig() {}

    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    const std::string& GetIndexCommonPath() const override;
    std::vector<std::string> GetIndexPath() const override;
    // wiil access by AddToUpdateDocumentRewriter
    std::vector<std::shared_ptr<config::FieldConfig>> GetFieldConfigs() const override { return {}; }
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override
    {
    }
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override {}
    void Check() const override { assert(false); }
    Status CheckCompatible(const IIndexConfig* other) const override { return Status::OK(); }
    bool IsDisabled() const override { return false; }

public:
    uint32_t GetBitmapSize() { return _bitmapSize; }
    void TEST_SetBitmapSize(uint32_t bitmapSize) { _bitmapSize = bitmapSize; }

private:
    uint32_t _bitmapSize = DEFAULT_BITMAP_SIZE;
};

} // namespace indexlibv2::index
