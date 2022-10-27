#ifndef __INDEXLIB_FIELD_SCHEMA_H
#define __INDEXLIB_FIELD_SCHEMA_H

#include <tr1/memory>
#include <string>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/enum_field_config.h"

DECLARE_REFERENCE_CLASS(config, FieldSchemaImpl);

IE_NAMESPACE_BEGIN(config);

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
    FieldConfigIteratorPtr CreateIterator(
            ConfigIteratorType type = CIT_NORMAL) const;
    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
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
    bool IsFieldTypeSortable(const std::string& fieldName, 
                             const std::string& fieldType) const;

private:
    FieldSchemaImplPtr mImpl;
    
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<FieldSchema> FieldSchemaPtr;

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_FIELD_SCHEMA_H
