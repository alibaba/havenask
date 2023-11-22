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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/modify_item.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class AttributeSchemaImpl;
typedef std::shared_ptr<AttributeSchemaImpl> AttributeSchemaImplPtr;
class AttributeSchema;
typedef std::shared_ptr<AttributeSchema> AttributeSchemaPtr;

class AttributeSchema : public autil::legacy::Jsonizable
{
public:
    typedef std::vector<AttributeConfigPtr> AttrVector;
    typedef AttrVector::const_iterator Iterator;
    typedef std::vector<AttributeConfigPtr> FieldId2AttrConfigVector;
    typedef std::map<std::string, attrid_t> NameMap;

public:
    AttributeSchema();
    ~AttributeSchema() {}

public:
    // return NULL if attribute is is_deleted or not exist
    // return object if attribute is is_normal or is_disable (maybe not ready)
    const AttributeConfigPtr& GetAttributeConfig(const std::string& attrName) const;
    const AttributeConfigPtr& GetAttributeConfig(attrid_t attrId) const;
    const AttributeConfigPtr& GetAttributeConfigByFieldId(fieldid_t fieldId) const;
    const AttrVector& GetAttributeConfigs() const;
    bool IsDeleted(attrid_t attrId) const;

    const PackAttributeConfigPtr& GetPackAttributeConfig(const std::string& packAttrName) const;

    const PackAttributeConfigPtr& GetPackAttributeConfig(packattrid_t packId) const;

    size_t GetPackAttributeCount() const;

    packattrid_t GetPackIdByAttributeId(attrid_t attrId) const;

    void AddAttributeConfig(const AttributeConfigPtr& attrConfig);
    void AddPackAttributeConfig(const PackAttributeConfigPtr& packAttrConfig);

    // attribute count include deleted & disabled attribute
    size_t GetAttributeCount() const;

    // Begin & End include deleted & disabled attribute
    Iterator Begin() const;
    Iterator End() const;

    AttributeConfigIteratorPtr CreateIterator(IndexStatus type = is_normal) const;
    PackAttributeConfigIteratorPtr CreatePackAttrIterator(IndexStatus type = is_normal) const;

    // iterator will access to target config object filtered by type
    bool IsInAttribute(fieldid_t fieldId) const;
    bool IsInAttribute(const std::string& attrName) const;

    void SetExistAttrConfig(fieldid_t fieldId, const AttributeConfigPtr& attrConf);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const AttributeSchema& other) const;
    void AssertCompatible(const AttributeSchema& other) const;
    bool HasSameAttributeConfigs(AttributeSchemaPtr& other) const;
    fieldid_t GetMaxFieldId() const;
    bool DisableAttribute(const std::string& attrName);
    bool DisablePackAttribute(const std::string& packAttrName);

    void DeleteAttribute(const std::string& attrName);
    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();
    void CollectBaseVersionAttrInfo(ModifyItemVector& attrs) const;

public:
    // for testlib
    const AttributeSchemaImplPtr& GetImpl();

private:
    AttributeSchemaImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AttributeSchema> AttributeSchemaPtr;
}} // namespace indexlib::config
