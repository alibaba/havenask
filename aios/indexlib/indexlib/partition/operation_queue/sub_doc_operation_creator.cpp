
#include "indexlib/partition/operation_queue/sub_doc_operation_creator.h"
#include "indexlib/partition/operation_queue/sub_doc_operation.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool; 
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocOperationCreator);

SubDocOperationCreator::SubDocOperationCreator(
        const IndexPartitionSchemaPtr& schema,
        OperationCreatorPtr mainCreator, OperationCreatorPtr subCreator)
    : OperationCreator(schema)
    , mMainCreator(mainCreator)
    , mSubCreator(subCreator)
{
    mMainPkType = schema->GetIndexSchema()->GetPrimaryKeyIndexType();
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    assert(subSchema);
    mSubPkType = subSchema->GetIndexSchema()->GetPrimaryKeyIndexType();
}

SubDocOperationCreator::~SubDocOperationCreator() 
{
}

OperationBase* SubDocOperationCreator::CreateMainOperation(
        const NormalDocumentPtr& doc, Pool* pool)
{
    if (mMainCreator)
    {
        return mMainCreator->Create(doc, pool);
    }
    return NULL;
}

OperationBase** SubDocOperationCreator::CreateSubOperation(
        const NormalDocumentPtr& doc, Pool* pool, size_t& subOperationCount)
{
    assert(mSubCreator);

    subOperationCount = 0;
    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    OperationBase** subOperations = NULL;
    if (subDocuments.size() > 0)
    {
        subOperations = IE_POOL_COMPATIBLE_NEW_VECTOR(
                pool, OperationBase*, subDocuments.size());
        if (!subOperations)
        {
            IE_LOG(ERROR, "allocate memory fail!");
            return NULL;
        }

        for (size_t i = 0; i < subDocuments.size(); ++i)
        {
            OperationBase* subOp = mSubCreator->Create(subDocuments[i], pool);
            if (!subOp)
            {
                //TODO: log level?
                IE_LOG(DEBUG, "failed to create sub operation");
                continue;
            }
            subOperations[subOperationCount++] = subOp;
        }
    }
    return subOperations;
}

OperationBase* SubDocOperationCreator::Create(
        const NormalDocumentPtr& doc, Pool* pool)
{
    if (!doc->HasPrimaryKey())
    {
        return NULL;
    }

    size_t subOperationCount = 0;
    OperationBase** subOperations = CreateSubOperation(
            doc, pool, subOperationCount);
    OperationBase* mainOp = CreateMainOperation(doc, pool);

    if (mainOp == NULL && subOperationCount == 0)
    {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(
                pool, subOperations, doc->GetSubDocuments().size());
        return NULL;
    }

    SubDocOperation *operation = 
        IE_POOL_COMPATIBLE_NEW_CLASS(
            pool, SubDocOperation, 
            doc->GetTimestamp(), mMainPkType, mSubPkType);
    
    operation->Init(doc->GetDocOperateType(), mainOp,
                    subOperations, subOperationCount);
    
    return operation;
}

IE_NAMESPACE_END(partition);

