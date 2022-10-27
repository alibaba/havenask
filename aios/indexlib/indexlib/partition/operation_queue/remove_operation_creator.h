#ifndef __INDEXLIB_REMOVE_OPERATION_CREATOR_H
#define __INDEXLIB_REMOVE_OPERATION_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_creator.h"

IE_NAMESPACE_BEGIN(partition);

class RemoveOperationCreator : public OperationCreator
{
public:
    RemoveOperationCreator(const config::IndexPartitionSchemaPtr& schema);
    ~RemoveOperationCreator();

public:
    OperationBase* Create(
            const document::NormalDocumentPtr& doc,
            autil::mem_pool::Pool *pool) override;

    autil::uint128_t GetPkHashFromOperation(OperationBase* operation, IndexType pkIndexType) const;
    
private:
    friend class RemoveOperationCreatorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RemoveOperationCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_REMOVE_OPERATION_CREATOR_H
