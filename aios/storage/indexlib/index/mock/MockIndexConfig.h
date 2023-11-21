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
#include "gmock/gmock.h"
#include <string>

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IIndexConfig.h"

namespace indexlibv2::config {

class MockIndexConfig : public IIndexConfig
{
public:
    MockIndexConfig() = default;
    virtual ~MockIndexConfig() = default;

    MOCK_METHOD(const std::string&, GetIndexType, (), (const, override));
    MOCK_METHOD(const std::string&, GetIndexName, (), (const, override));
    MOCK_METHOD(const std::string&, GetIndexCommonPath, (), (const, override));
    MOCK_METHOD(std::vector<std::string>, GetIndexPath, (), (const, override));
    MOCK_METHOD(bool, IsDisabled, (), (const, override));
    MOCK_METHOD(std::vector<std::shared_ptr<config::FieldConfig>>, GetFieldConfigs, (), (const, override));
    MOCK_METHOD(void, Deserialize,
                (const autil::legacy::Any& any, size_t idxInJsonArray, const IndexConfigDeserializeResource& resource),
                (override));
    MOCK_METHOD(void, Serialize, (autil::legacy::Jsonizable::JsonWrapper & json), (const, override));
    MOCK_METHOD(void, Check, (), (const, override));
    MOCK_METHOD(Status, CheckCompatible, (const IIndexConfig* other), (const, override));
};

} // namespace indexlibv2::config
