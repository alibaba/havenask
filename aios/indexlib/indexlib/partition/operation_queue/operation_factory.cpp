#include "indexlib/partition/operation_queue/operation_factory.h"
#include "indexlib/partition/operation_queue/remove_operation_creator.h"
#include "indexlib/partition/operation_queue/update_field_operation_creator.h"
#include "indexlib/partition/operation_queue/sub_doc_operation_creator.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OperationFactory);

OperationCreatorPtr OperationFactory::CreateRemoveOperationCreator(
        const IndexPartitionSchemaPtr& schema)
{
    return OperationCreatorPtr(new RemoveOperationCreator(schema));
}

OperationCreatorPtr OperationFactory::CreateUpdateFieldOperationCreator(
        const IndexPartitionSchemaPtr& schema)
{
    return OperationCreatorPtr(new UpdateFieldOperationCreator(schema));
}

void OperationFactory::Init(const IndexPartitionSchemaPtr& schema)
{
    if (schema->GetIndexSchema()->HasPrimaryKeyIndex())
    {
        mMainPkType =  schema->GetIndexSchema()->GetPrimaryKeyIndexType();
    }
    
    const IndexPartitionSchemaPtr& subSchema = 
        schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        mUpdateOperationCreator.reset(new SubDocOperationCreator(schema, 
                        CreateUpdateFieldOperationCreator(schema),
                        CreateUpdateFieldOperationCreator(subSchema)));
        mRemoveSubOperationCreator.reset(new SubDocOperationCreator(schema, 
                        OperationCreatorPtr(),
                        CreateRemoveOperationCreator(subSchema)));
        if (subSchema->GetIndexSchema()->HasPrimaryKeyIndex())
        {
            mSubPkType =  subSchema->GetIndexSchema()->GetPrimaryKeyIndexType();
            assert(mSubPkType == it_primarykey64 || mSubPkType == it_primarykey128);
        }
    }
    else
    {
        mUpdateOperationCreator = CreateUpdateFieldOperationCreator(schema);
    }
    mRemoveOperationCreator = CreateRemoveOperationCreator(schema);    
}

OperationBase* OperationFactory::CreateOperation(
        const NormalDocumentPtr& doc, Pool *pool)
{
    DocOperateType opType = doc->GetDocOperateType();
    if (opType == ADD_DOC || opType == DELETE_DOC)
    {
        return mRemoveOperationCreator->Create(doc, pool);
    }

    if (opType == UPDATE_FIELD)
    {
        return mUpdateOperationCreator->Create(doc, pool);
    }

    if (opType == DELETE_SUB_DOC && mRemoveSubOperationCreator)
    {
        return mRemoveSubOperationCreator->Create(doc, pool);
    }

    return NULL;
}


IE_NAMESPACE_END(partition);

