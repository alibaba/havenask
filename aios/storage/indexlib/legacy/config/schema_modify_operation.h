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
#include "indexlib/config/modify_item.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class SchemaModifyOperationImpl;
typedef std::shared_ptr<SchemaModifyOperationImpl> SchemaModifyOperationImplPtr;
} // namespace indexlib::config
namespace indexlib::config {
class IndexConfig;
typedef std::shared_ptr<IndexConfig> IndexConfigPtr;
} // namespace indexlib::config
namespace indexlib::config {
class AttributeConfig;
typedef std::shared_ptr<AttributeConfig> AttributeConfigPtr;
} // namespace indexlib::config
namespace indexlib::config {
class FieldConfig;
typedef std::shared_ptr<FieldConfig> FieldConfigPtr;
} // namespace indexlib::config

namespace indexlib { namespace config {

class IndexPartitionSchemaImpl;
class SchemaModifyOperation : public autil::legacy::Jsonizable
{
public:
    SchemaModifyOperation();
    ~SchemaModifyOperation();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    schema_opid_t GetOpId() const;
    void SetOpId(schema_opid_t id);

    void LoadDeleteOperation(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);

    void LoadAddOperation(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);

    void MarkNotReady();
    bool IsNotReady() const;

    void CollectEffectiveModifyItem(ModifyItemVector& indexs, ModifyItemVector& attrs);
    void AssertEqual(const SchemaModifyOperation& other) const;

    const std::map<std::string, std::string>& GetParams() const;
    void SetParams(const std::map<std::string, std::string>& params);

    void Validate() const;

public:
    const std::vector<FieldConfigPtr>& GetAddFields() const;
    const std::vector<IndexConfigPtr>& GetAddIndexs() const;
    const std::vector<AttributeConfigPtr>& GetAddAttributes() const;
    const std::vector<std::string>& GetDeleteFields() const;
    const std::vector<std::string>& GetDeleteIndexs() const;
    const std::vector<std::string>& GetDeleteAttrs() const;

private:
    SchemaModifyOperationImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SchemaModifyOperation> SchemaModifyOperationPtr;
}} // namespace indexlib::config
