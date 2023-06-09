/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "indexlib/partition/operation_queue/sub_doc_operation_creator.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/partition/operation_queue/sub_doc_operation.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::config;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SubDocOperationCreator);

SubDocOperationCreator::SubDocOperationCreator(const IndexPartitionSchemaPtr& schema, OperationCreatorPtr mainCreator,
                                               OperationCreatorPtr subCreator)
    : OperationCreator(schema)
    , mMainCreator(mainCreator)
    , mSubCreator(subCreator)
{
    mMainPkType = schema->GetIndexSchema()->GetPrimaryKeyIndexType();
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    assert(subSchema);
    mSubPkType = subSchema->GetIndexSchema()->GetPrimaryKeyIndexType();
}

SubDocOperationCreator::~SubDocOperationCreator() {}

bool SubDocOperationCreator::CreateMainOperation(const NormalDocumentPtr& doc, Pool* pool, OperationBase** operation)
{
    if (mMainCreator) {
        return mMainCreator->Create(doc, pool, operation);
    }
    *operation = nullptr;
    return true;
}

OperationBase** SubDocOperationCreator::CreateSubOperation(const NormalDocumentPtr& doc, Pool* pool,
                                                           size_t& subOperationCount)
{
    assert(mSubCreator);

    subOperationCount = 0;
    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    OperationBase** subOperations = NULL;
    if (subDocuments.size() > 0) {
        subOperations = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationBase*, subDocuments.size());
        if (!subOperations) {
            IE_LOG(ERROR, "allocate memory fail!");
            return NULL;
        }

        for (size_t i = 0; i < subDocuments.size(); ++i) {
            OperationBase* op = nullptr;
            if (!mSubCreator->Create(subDocuments[i], pool, &op)) {
                // TODO: log level?
                IE_LOG(WARN, "failed to create sub operation");
                continue;
            }
            if (!op) {
                IE_LOG(DEBUG, "failed to create sub operation");
                continue;
            }
            subOperations[subOperationCount++] = op;
        }
    }
    return subOperations;
}

bool SubDocOperationCreator::Create(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                                    OperationBase** operation)
{
    if (!doc->HasPrimaryKey()) {
        return false;
    }

    size_t subOperationCount = 0;
    OperationBase** subOperations = CreateSubOperation(doc, pool, subOperationCount);
    OperationBase* mainOp = nullptr;
    if (!CreateMainOperation(doc, pool, &mainOp)) {
        return false;
    }
    if (subOperationCount == 0 && mainOp == nullptr) {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, subOperations, doc->GetSubDocuments().size());
        return true;
    }

    SubDocOperation* subDocOp =
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, SubDocOperation, doc->GetTimestamp(), mMainPkType, mSubPkType);
    subDocOp->Init(doc->GetDocOperateType(), mainOp, subOperations, subOperationCount);
    *operation = subDocOp;
    return true;
}
}} // namespace indexlib::partition
