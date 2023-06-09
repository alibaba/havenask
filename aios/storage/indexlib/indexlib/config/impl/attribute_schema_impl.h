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
#ifndef __INDEXLIB_ATTRIBUTE_SCHEMA_IMPL_H
#define __INDEXLIB_ATTRIBUTE_SCHEMA_IMPL_H

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace config {
class AttributeSchemaImpl;
DEFINE_SHARED_PTR(AttributeSchemaImpl);

class AttributeSchemaImpl : public autil::legacy::Jsonizable
{
public:
    typedef std::vector<AttributeConfigPtr> AttrVector;
    typedef AttrVector::const_iterator Iterator;
    typedef std::vector<AttributeConfigPtr> FieldId2AttrConfigVector;
    typedef std::map<std::string, attrid_t> NameMap;

public:
    AttributeSchemaImpl();
    ~AttributeSchemaImpl();

public:
    const AttributeConfigPtr& GetAttributeConfig(const std::string& attrName) const;
    const AttributeConfigPtr& GetAttributeConfig(attrid_t attrId) const;
    const AttributeConfigPtr& GetAttributeConfigByFieldId(fieldid_t fieldId) const;
    const AttrVector& GetAttributeConfigs() const;
    bool IsDeleted(attrid_t attrId) const;

    const PackAttributeConfigPtr& GetPackAttributeConfig(const std::string& packAttrName) const;

    const PackAttributeConfigPtr& GetPackAttributeConfig(packattrid_t packId) const;

    size_t GetPackAttributeCount() const { return mPackAttrConfigs.size(); }

    packattrid_t GetPackIdByAttributeId(attrid_t attrId) const;

    void AddAttributeConfig(const AttributeConfigPtr& attrConfig);
    void AddPackAttributeConfig(const PackAttributeConfigPtr& packAttrConfig);

    size_t GetAttributeCount() const { return mAttrConfigs.size(); }

    AttributeSchema::Iterator Begin() const { return mAttrConfigs.begin(); }
    AttributeSchema::Iterator End() const { return mAttrConfigs.end(); }
    AttributeConfigIteratorPtr CreateIterator(IndexStatus type) const;
    PackAttributeConfigIteratorPtr CreatePackAttrIterator(IndexStatus type) const;

    bool IsInAttribute(fieldid_t fieldId) const;
    bool IsInAttribute(const std::string& attrName) const;
    void SetExistAttrConfig(fieldid_t fieldId, const AttributeConfigPtr& attrConf);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const AttributeSchemaImpl& other) const;
    void AssertCompatible(const AttributeSchemaImpl& other) const;
    bool HasSameAttributeConfigs(const AttributeSchemaImplPtr& other) const;
    fieldid_t GetMaxFieldId() const { return mMaxFieldId; }
    bool DisableAttribute(const std::string& attrName);
    bool DisablePackAttribute(const std::string& packAttrName);

    void DeleteAttribute(const std::string& attrName);
    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();

    void CollectBaseVersionAttrInfo(ModifyItemVector& attrs) const;

private:
    attrid_t GetAttributeId(const std::string& attrName) const;
    void AddAttributeConfig(const AttributeConfigPtr& attrConfig, NameMap& nameMap, AttrVector& attrConfigs);

private:
    AttrVector mAttrConfigs;
    NameMap mAttrName2IdMap;

    std::vector<PackAttributeConfigPtr> mPackAttrConfigs;
    NameMap mPackAttrName2IdMap;

    // just for IsInAttribute
    FieldId2AttrConfigVector mFieldId2AttrConfigVec;
    fieldid_t mMaxFieldId;
    int32_t mBaseAttrCount;
    bool mOnlyAddVirtual;

private:
    static AttributeConfigPtr NULL_ATTR_CONF;
    static PackAttributeConfigPtr NULL_PACK_ATTR_CONF;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeSchemaImpl);

/////////////////////////////////////////////////////////////////////
inline const AttributeConfigPtr& AttributeSchemaImpl::GetAttributeConfig(attrid_t attrId) const
{
    if (attrId == INVALID_ATTRID) {
        return NULL_ATTR_CONF;
    }
    if (attrId < (attrid_t)mAttrConfigs.size()) {
        const AttributeConfigPtr& ret = mAttrConfigs[attrId];
        if (ret && !ret->IsDeleted()) {
            return ret;
        }
        return NULL_ATTR_CONF;
    }
    return NULL_ATTR_CONF;
}

inline bool AttributeSchemaImpl::IsDeleted(attrid_t attrId) const
{
    if (attrId < 0 || attrId >= (attrid_t)mAttrConfigs.size()) {
        return false;
    }
    return mAttrConfigs[attrId]->IsDeleted();
}

inline bool AttributeSchemaImpl::IsInAttribute(const std::string& attrName) const
{
    return GetAttributeConfig(attrName) != NULL_ATTR_CONF;
}

inline bool AttributeSchemaImpl::IsInAttribute(fieldid_t fieldId) const
{
    if (fieldId > mMaxFieldId || fieldId == INVALID_FIELDID) {
        return false;
    }

    auto ret = mFieldId2AttrConfigVec[fieldId];
    return ret && !ret->IsDeleted();
}

inline const AttributeConfigPtr& AttributeSchemaImpl::GetAttributeConfigByFieldId(fieldid_t fieldId) const
{
    if (fieldId > mMaxFieldId || fieldId == INVALID_FIELDID) {
        return NULL_ATTR_CONF;
    }

    const AttributeConfigPtr& ret = mFieldId2AttrConfigVec[fieldId];
    if (ret && !ret->IsDeleted()) {
        return ret;
    }
    return NULL_ATTR_CONF;
}

inline const PackAttributeConfigPtr& AttributeSchemaImpl::GetPackAttributeConfig(const std::string& packAttrName) const
{
    NameMap::const_iterator it = mPackAttrName2IdMap.find(packAttrName);
    if (it != mPackAttrName2IdMap.end()) {
        assert(it->second < mPackAttrConfigs.size());
        return mPackAttrConfigs[it->second];
    } else {
        return NULL_PACK_ATTR_CONF;
    }
}

inline const PackAttributeConfigPtr& AttributeSchemaImpl::GetPackAttributeConfig(packattrid_t packId) const
{
    if (packId == INVALID_PACK_ATTRID || packId >= mPackAttrConfigs.size()) {
        return NULL_PACK_ATTR_CONF;
    }
    return mPackAttrConfigs[packId];
}

inline packattrid_t AttributeSchemaImpl::GetPackIdByAttributeId(attrid_t attrId) const
{
    const AttributeConfigPtr& attrConfig = GetAttributeConfig(attrId);
    if (attrConfig == NULL_ATTR_CONF) {
        return INVALID_PACK_ATTRID;
    }
    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (!packAttrConfig) {
        return INVALID_PACK_ATTRID;
    }
    return packAttrConfig->GetPackAttrId();
}
}} // namespace indexlib::config

#endif //__INDEXLIB_ATTRIBUTE_SCHEMA_IMPL_H
