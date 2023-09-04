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
#include "gmock/gmock.h"
#include <memory>
#include <string>

#include "indexlib/config/TabletSchema.h"

namespace indexlibv2::framework {

class MockTabletSchema : public config::TabletSchema
{
public:
    MOCK_METHOD(const std::string&, GetSchemaNameImpl, (), (const));
    MOCK_METHOD(bool, SerializeImpl, (std::string*), (const));
    MOCK_METHOD(bool, DeserializeImpl, (const std::string&), (const));
    MOCK_METHOD(std::vector<std::shared_ptr<config::IIndexConfig>>, GetIndexConfigs, (), (const, override));
    MOCK_METHOD(std::vector<std::shared_ptr<config::IIndexConfig>>, GetIndexConfigs, (const std::string&),
                (const, override));

public:
    const std::string& GetTableName() const override { return GetSchemaNameImpl(); }
    std::string GetSchemaFileName() const override { return "schema.json"; }
    const std::string& GetTableType() const override
    {
        static std::string mock("mock");
        return mock;
    }
    bool Serialize(bool isCompact, std::string* output) const override { return SerializeImpl(output); }
    bool Deserialize(const std::string& data) override { return DeserializeImpl(data); }
    schemaid_t GetSchemaId() const override { return 0; }
};
} // namespace indexlibv2::framework
