#ifndef __INDEXLIB_UPDATE_FIELD_EXTRACTOR_H
#define __INDEXLIB_UPDATE_FIELD_EXTRACTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/index_define.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/util/slice_array/defrag_slice_array.h"

DECLARE_REFERENCE_CLASS(document, AttributeDocument);

IE_NAMESPACE_BEGIN(index);

class UpdateFieldExtractor
{
private:
    typedef std::pair<fieldid_t, autil::ConstString> FieldItem;
    typedef std::vector<FieldItem> FieldVector;
public:
    class Iterator
    {
    public:
        Iterator(const FieldVector& fieldVec) 
            : mFields(fieldVec)
        {
            mCursor = mFields.begin();
        }

    public:
        bool HasNext() const 
        {
            return mCursor != mFields.end(); 
        }
        
        const autil::ConstString& Next(fieldid_t& fieldId)
        {
            assert(mCursor != mFields.end());
            fieldId = mCursor->first;
            const autil::ConstString& value = mCursor->second;
            mCursor++;
            return value;
        }

    private:
        const FieldVector& mFields;
        FieldVector::const_iterator mCursor;
    };

public:
    UpdateFieldExtractor(const config::IndexPartitionSchemaPtr& schema)
        : mSchema(schema)
    {
        const config::IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        assert(indexSchema);
        mPrimaryKeyFieldId = indexSchema->GetPrimaryKeyIndexFieldId();
    }

    ~UpdateFieldExtractor() {}

public:
    bool Init(const document::AttributeDocumentPtr& attrDoc);
    size_t GetFieldCount() const 
    { return mFieldVector.size(); }
    Iterator CreateIterator() const
    { return Iterator(mFieldVector); }

private:
    bool IsFieldIgnore(const config::FieldConfigPtr& fieldConfig,
                       const autil::ConstString& fieldValue);

private:
    config::IndexPartitionSchemaPtr mSchema;
    fieldid_t mPrimaryKeyFieldId;
    FieldVector mFieldVector;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UpdateFieldExtractor);
/////////////////////////////////////////////////////////////
inline bool UpdateFieldExtractor::IsFieldIgnore(
        const config::FieldConfigPtr& fieldConfig,
        const autil::ConstString& fieldValue)
{
    if (fieldValue.empty())
    {
        return true;
    }

    const std::string& fieldName = fieldConfig->GetFieldName();
    fieldid_t fieldId = fieldConfig->GetFieldId();
    if (fieldId == mPrimaryKeyFieldId)
    {
        IE_LOG(DEBUG, "field[%s] is primarykey field, ignore", fieldName.c_str());
        return true;
    }

    if (!fieldConfig->IsAttributeUpdatable())
    {
        IE_LOG(WARN, "Unsupported updatable field[%s], ignore",
               fieldName.c_str());
        ERROR_COLLECTOR_LOG(WARN, "Unsupported updatable field[%s], ignore",
               fieldName.c_str());
        return true;
    }

    const config::AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    assert(attrSchema);
    if (!attrSchema->IsInAttribute(fieldId))
    {
        IE_LOG(DEBUG, "no attribute for field[%s], ignore", fieldName.c_str());
        ERROR_COLLECTOR_LOG(WARN, "no attribute for field[%s], ignore", fieldName.c_str());
        return true;
    }

    // TODO: remove when defrag slice array support length over 64MB
    if (util::DefragSliceArray::IsOverLength(
                    fieldValue.size(), common::VAR_NUM_ATTRIBUTE_SLICE_LEN))
    {
        IE_LOG(DEBUG, "attribute for field[%s] is overlength, ignore",
               fieldName.c_str());
        ERROR_COLLECTOR_LOG(WARN, "attribute for field[%s] is overlength, ignore", 
               fieldName.c_str());
        return true;
    }
    return false;
}


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_UPDATE_FIELD_EXTRACTOR_H
