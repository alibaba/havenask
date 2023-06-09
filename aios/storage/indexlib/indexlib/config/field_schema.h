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
#ifndef __INDEXLIB_FIELD_SCHEMA_H
#define __INDEXLIB_FIELD_SCHEMA_H

#include <memory>
#include <string>

#include "indexlib/common_define.h"
#include "indexlib/config/enum_field_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, FieldSchemaImpl);

namespace indexlib { namespace config {

class IndexPartitionSchemaImpl;

class FieldSchema : public autil::legacy::Jsonizable
{
public:
    typedef std::vector<FieldConfigPtr>::const_iterator Iterator;
    typedef std::map<std::string, fieldid_t> NameMap;

public:
    FieldSchema(size_t capacity = 4);
    ~FieldSchema() {}

public:
    void AddFieldConfig(const FieldConfigPtr& fieldConfig);

    // return NULL if field is is_deleted
    // return object if field is is_normal

    FieldConfigPtr GetFieldConfig(fieldid_t fieldId) const;
    FieldConfigPtr GetFieldConfig(const std::string& fieldName) const;
    bool IsDeleted(fieldid_t fieldId) const;

    // return INVALID_FIELDID if field is deleted
    fieldid_t GetFieldId(const std::string& fieldName) const;

    // field count include deleted
    size_t GetFieldCount() const;

    // Begin & End include deleted & disabled field
    Iterator Begin() const;
    Iterator End() const;

    // iterator will access to target config object filtered by type
    FieldConfigIteratorPtr CreateIterator(IndexStatus type = is_normal) const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const FieldSchema& other) const;
    void AssertCompatible(const FieldSchema& other) const;

    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();
    void DeleteField(const std::string& field);
    void RewriteFieldType();

public:
    // not include deleted field
    bool IsFieldNameInSchema(const std::string& fieldName) const;
    bool IsFieldTypeSortable(const std::string& fieldName, const std::string& fieldType) const;

private:
    FieldSchemaImplPtr mImpl;
    const std::vector<std::shared_ptr<FieldConfig>>& GetFieldConfigs() const;

    friend class indexlib::config::IndexPartitionSchemaImpl;

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<FieldSchema> FieldSchemaPtr;
}} // namespace indexlib::config

#endif //__INDEXLIB_FIELD_SCHEMA_H
