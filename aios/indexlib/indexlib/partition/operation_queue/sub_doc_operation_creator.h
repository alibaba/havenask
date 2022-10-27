#ifndef __INDEXLIB_SUB_DOC_OPERATION_CREATOR_H
#define __INDEXLIB_SUB_DOC_OPERATION_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_creator.h"

IE_NAMESPACE_BEGIN(partition);

class SubDocOperationCreator : public OperationCreator
{
public:
    SubDocOperationCreator(
            const config::IndexPartitionSchemaPtr& schema,
            OperationCreatorPtr mainCreator, OperationCreatorPtr subCreator);
    ~SubDocOperationCreator();
public:
    OperationBase* Create(const document::NormalDocumentPtr& doc,
                          autil::mem_pool::Pool* pool) override;

private:
    OperationBase* CreateMainOperation(const document::NormalDocumentPtr& doc, 
            autil::mem_pool::Pool* pool);
    OperationBase** CreateSubOperation(const document::NormalDocumentPtr& doc,
            autil::mem_pool::Pool* pool, size_t& subOperationCount);

private:
    OperationCreatorPtr mMainCreator;
    OperationCreatorPtr mSubCreator;
    IndexType mMainPkType;
    IndexType mSubPkType;
private:
    friend class SubDocOperationCreatorTest;
    friend class OperationFactoryTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocOperationCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUB_DOC_OPERATION_CREATOR_H
