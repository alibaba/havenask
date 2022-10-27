
#include "indexlib/partition/operation_queue/remove_operation_creator.h"
#include "indexlib/partition/operation_queue/remove_operation.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, RemoveOperationCreator);

RemoveOperationCreator::RemoveOperationCreator(const IndexPartitionSchemaPtr& schema)
    : OperationCreator(schema)
{
}

RemoveOperationCreator::~RemoveOperationCreator() 
{
}

OperationBase* RemoveOperationCreator::Create(
        const NormalDocumentPtr& doc, Pool *pool)
{
    assert(doc->HasPrimaryKey());
    uint128_t pkHash;
    GetPkHash(doc->GetIndexDocument(), pkHash);
    segmentid_t segmentId = doc->GetSegmentIdBeforeModified();

    OperationBase *operation = NULL;
    if (GetPkIndexType() == it_primarykey64)
    {
        RemoveOperation<uint64_t> *removeOperation = 
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<uint64_t>, 
                                      doc->GetTimestamp());
        removeOperation->Init(pkHash.value[1], segmentId);
        operation = removeOperation;
    }
    else
    {
        RemoveOperation<uint128_t> *removeOperation = 
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<autil::uint128_t>, 
                                      doc->GetTimestamp());
        removeOperation->Init(pkHash, segmentId);
        operation = removeOperation;
   } 

    if (!operation)
    {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    return operation;
}

uint128_t RemoveOperationCreator::GetPkHashFromOperation(
    OperationBase* operation, IndexType pkIndexType) const
{
    assert(operation);
    if (pkIndexType == it_primarykey64)
    {
        RemoveOperation<uint64_t>* typed64Op =
            static_cast<RemoveOperation<uint64_t>*>(operation);
        return typed64Op->GetPkHash();
    }
    RemoveOperation<uint128_t>* typed128Op =
        static_cast<RemoveOperation<autil::uint128_t>*>(operation);
    return typed128Op->GetPkHash();        
}

IE_NAMESPACE_END(partition);

