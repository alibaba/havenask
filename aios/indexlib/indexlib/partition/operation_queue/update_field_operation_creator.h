#ifndef __INDEXLIB_UPDATE_FIELD_OPERATION_CREATOR_H
#define __INDEXLIB_UPDATE_FIELD_OPERATION_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_creator.h"
#include "indexlib/partition/operation_queue/update_field_operation.h"

DECLARE_REFERENCE_CLASS(common, AttributeConvertor);
IE_NAMESPACE_BEGIN(partition);

class UpdateFieldOperationCreator : public OperationCreator
{
private:
    typedef std::vector<common::AttributeConvertorPtr> FieldIdToConvertorMap;

public:
    UpdateFieldOperationCreator(const config::IndexPartitionSchemaPtr& schema);
    ~UpdateFieldOperationCreator();

public:
    OperationBase* Create(
            const document::NormalDocumentPtr& doc,
            autil::mem_pool::Pool *pool) override;

private:
    OperationItem* CreateOperationItems(
            autil::mem_pool::Pool *pool, 
            const document::AttributeDocumentPtr& attrDoc,
            uint32_t& itemCount);

    autil::ConstString DeserializeField(
            fieldid_t fieldId, const autil::ConstString& fieldValue);

    void InitAttributeConvertorMap(
            const config::IndexPartitionSchemaPtr& schema);

private:
    FieldIdToConvertorMap mFieldId2ConvertorMap;

private:
    friend class UpdateFieldOperationCreatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UpdateFieldOperationCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_UPDATE_FIELD_OPERATION_CREATOR_H
