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
#include <string>

#include "autil/Log.h"
#include "indexlib/config/enum_field_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlibv2 { namespace config {
class LegacySchemaConvertor;
}} // namespace indexlibv2::config

namespace indexlib { namespace config {

class FieldSchemaImpl : public autil::legacy::Jsonizable
{
public:
    typedef std::vector<FieldConfigPtr>::const_iterator Iterator;
    typedef std::map<std::string, fieldid_t> NameMap;

public:
    FieldSchemaImpl(size_t capacity = 4);
    ~FieldSchemaImpl() {}

public:
    void AddFieldConfig(const FieldConfigPtr& fieldConfig);
    const std::vector<std::shared_ptr<FieldConfig>>& GetFieldConfigs() const { return mFields; }
    FieldConfigPtr GetFieldConfig(fieldid_t fieldId) const;
    FieldConfigPtr GetFieldConfig(const std::string& fieldName) const;
    fieldid_t GetFieldId(const std::string& fieldName) const;
    size_t GetFieldCount() const { return mFields.size(); }
    bool IsDeleted(fieldid_t fieldId) const;

    Iterator Begin() const { return mFields.begin(); }
    Iterator End() const { return mFields.end(); }
    FieldConfigIteratorPtr CreateIterator(IndexStatus type) const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const FieldSchemaImpl& other) const;
    void AssertCompatible(const FieldSchemaImpl& other) const;
    void DeleteField(const std::string& field);
    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();
    void RewriteFieldType();

public:
    bool IsFieldNameInSchema(const std::string& fieldName) const;
    bool IsFieldTypeSortable(const std::string& fieldName, const std::string& fieldType) const;

private:
    void AddFieldConfig(const FieldConfigPtr& fieldConfig, NameMap& nameMap, std::vector<FieldConfigPtr>& fields);

private:
    std::vector<FieldConfigPtr> mFields;
    NameMap mNameToIdMap;
    int32_t mBaseFieldCount;
    bool mOnlyAddVirtual;

private:
    friend class indexlibv2::config::LegacySchemaConvertor;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FieldSchemaImpl> FieldSchemaImplPtr;
}} // namespace indexlib::config
