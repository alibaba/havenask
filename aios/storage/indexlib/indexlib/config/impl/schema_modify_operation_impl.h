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
#ifndef __INDEXLIB_SCHEMA_MODIFY_OPERATION_IMPL_H
#define __INDEXLIB_SCHEMA_MODIFY_OPERATION_IMPL_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/modify_item.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(test, ModifySchemaMaker);

namespace indexlib { namespace config {

class IndexPartitionSchemaImpl;
class SchemaModifyOperationImpl : public autil::legacy::Jsonizable
{
public:
    SchemaModifyOperationImpl();
    ~SchemaModifyOperationImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    schema_opid_t GetOpId() const;
    void SetOpId(schema_opid_t id);

    void LoadDeleteOperation(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);

    void LoadAddOperation(const autil::legacy::Any& any, IndexPartitionSchemaImpl& schema);

    void MarkNotReady();
    bool IsNotReady() const { return mNotReady; }

    void CollectEffectiveModifyItem(ModifyItemVector& indexs, ModifyItemVector& attrs);

    void AssertEqual(const SchemaModifyOperationImpl& other) const;

    void Validate() const;

    const std::map<std::string, std::string>& GetParams() const { return mParameters; }

    void SetParams(const std::map<std::string, std::string>& params) { mParameters = params; }

    const std::vector<FieldConfigPtr>& GetAddFields() const { return mAddFields; }
    const std::vector<IndexConfigPtr>& GetAddIndexs() const { return mAddIndexs; }
    const std::vector<AttributeConfigPtr>& GetAddAttributes() const { return mAddAttrs; }
    const std::vector<std::string>& GetDeleteFields() const { return mDeleteFields; }
    const std::vector<std::string>& GetDeleteIndexs() const { return mDeleteIndexs; }
    const std::vector<std::string>& GetDeleteAttrs() const { return mDeleteAttrs; }

private:
    void EnsureAccompanyAttributeDeleteForSpatialIndex(const std::string& indexName, IndexPartitionSchemaImpl& schema);

    void InitTruncateSortFieldSet(const IndexPartitionSchemaImpl& schema, std::set<std::string>& truncateSortFields);

private:
    std::vector<std::string> mDeleteFields;
    std::vector<std::string> mDeleteIndexs;
    std::vector<std::string> mDeleteAttrs;

    std::vector<FieldConfigPtr> mAddFields;
    std::vector<IndexConfigPtr> mAddIndexs;
    std::vector<AttributeConfigPtr> mAddAttrs;

    schema_opid_t mOpId;
    bool mNotReady; /* add op not ready, delete op take effect immediately */

    std::map<std::string, std::string> mParameters;

private:
    friend class test::ModifySchemaMaker;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaModifyOperationImpl);
}} // namespace indexlib::config

#endif //__INDEXLIB_SCHEMA_MODIFY_OPERATION_IMPL_H
