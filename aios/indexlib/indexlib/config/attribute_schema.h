#ifndef __INDEXLIB_ATTRIBUTE_SCHEMA_H
#define __INDEXLIB_ATTRIBUTE_SCHEMA_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <tr1/memory>
#include <string>
#include <vector>
#include <map>
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/modify_item.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);

class AttributeSchemaImpl;
DEFINE_SHARED_PTR(AttributeSchemaImpl);
class AttributeSchema;
DEFINE_SHARED_PTR(AttributeSchema);

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

    const PackAttributeConfigPtr&
        GetPackAttributeConfig(const std::string& packAttrName) const;

    const PackAttributeConfigPtr&
        GetPackAttributeConfig(packattrid_t packId) const;

    size_t GetPackAttributeCount() const;

    packattrid_t GetPackIdByAttributeId(attrid_t attrId) const;

    void AddAttributeConfig(const AttributeConfigPtr& attrConfig);
    void AddPackAttributeConfig(const PackAttributeConfigPtr& packAttrConfig);

    // attribute count include deleted & disabled attribute    
    size_t GetAttributeCount() const; 

    // Begin & End include deleted & disabled attribute    
    Iterator Begin() const;
    Iterator End() const;
    
    AttributeConfigIteratorPtr CreateIterator(ConfigIteratorType type = CIT_NORMAL) const;
    PackAttributeConfigIteratorPtr CreatePackAttrIterator(ConfigIteratorType type = CIT_NORMAL) const;

    // iterator will access to target config object filtered by type    
    bool IsInAttribute(fieldid_t fieldId) const;
    bool IsInAttribute(const std::string &attrName) const;
    
    void SetExistAttrConfig(fieldid_t fieldId, const AttributeConfigPtr& attrConf);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
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
    //for testlib
    const AttributeSchemaImplPtr& GetImpl();
private:
    AttributeSchemaImplPtr mImpl;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeSchema);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_ATTRIBUTE_SCHEMA_H
