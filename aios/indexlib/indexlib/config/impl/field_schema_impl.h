#ifndef __INDEXLIB_FIELD_SCHEMA_IMPL_H
#define __INDEXLIB_FIELD_SCHEMA_IMPL_H

#include <tr1/memory>
#include <string>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/config/field_config.h"
#include "indexlib/config/enum_field_config.h"

IE_NAMESPACE_BEGIN(config);

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
    FieldConfigPtr GetFieldConfig(fieldid_t fieldId) const;
    FieldConfigPtr GetFieldConfig(const std::string& fieldName) const;
    fieldid_t GetFieldId(const std::string& fieldName) const;
    size_t GetFieldCount() const { return mFields.size(); }
    bool IsDeleted(fieldid_t fieldId) const;
    
    Iterator Begin() const { return mFields.begin();}
    Iterator End() const { return mFields.end();}
    FieldConfigIteratorPtr CreateIterator(ConfigIteratorType type) const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    void AssertEqual(const FieldSchemaImpl& other) const;
    void AssertCompatible(const FieldSchemaImpl& other) const;
    void DeleteField(const std::string& field);
    void SetBaseSchemaImmutable();
    void SetModifySchemaImmutable();
    void SetModifySchemaMutable();
    void RewriteFieldType();    
public:
    bool IsFieldNameInSchema(const std::string& fieldName) const;
    bool IsFieldTypeSortable(const std::string& fieldName, 
                             const std::string& fieldType) const;

private:
    void AddFieldConfig(const FieldConfigPtr& fieldConfig,
                        NameMap& nameMap,
                        std::vector<FieldConfigPtr>& fields);

private:
    std::vector<FieldConfigPtr> mFields;
    NameMap mNameToIdMap;
    int32_t mBaseFieldCount;
    bool mOnlyAddVirtual;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<FieldSchemaImpl> FieldSchemaImplPtr;

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_FIELD_SCHEMA_IMPL_H
