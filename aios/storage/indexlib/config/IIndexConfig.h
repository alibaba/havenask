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

#include <string>

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/FieldConfig.h"

namespace indexlibv2::config {
class IndexConfigDeserializeResource;

class IIndexConfig
{
public:
    IIndexConfig() = default;
    virtual ~IIndexConfig() = default;

public:
    virtual const std::string& GetIndexType() const = 0;
    virtual const std::string& GetIndexName() const = 0;
    virtual const std::string& GetIndexCommonPath() const = 0; // only related to index type
    virtual std::vector<std::string> GetIndexPath() const = 0; //获取index从segment开始的相对路径
    virtual std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const = 0;
    virtual void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                             const IndexConfigDeserializeResource& resource) = 0;
    virtual void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const = 0;
    virtual void Check() const = 0;
    // check compatible when alter table
    virtual Status CheckCompatible(const IIndexConfig* other) const = 0;
    virtual bool IsDisabled() const = 0;
};

} // namespace indexlibv2::config
