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
#include "indexlib/partition/operation_queue/operation_factory.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/partition/operation_queue/remove_operation_creator.h"
#include "indexlib/partition/operation_queue/sub_doc_operation_creator.h"
#include "indexlib/partition/operation_queue/update_field_operation_creator.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OperationFactory);

OperationCreatorPtr OperationFactory::CreateRemoveOperationCreator(const IndexPartitionSchemaPtr& schema)
{
    return OperationCreatorPtr(new RemoveOperationCreator(schema));
}

OperationCreatorPtr OperationFactory::CreateUpdateFieldOperationCreator(const IndexPartitionSchemaPtr& schema)
{
    return OperationCreatorPtr(new UpdateFieldOperationCreator(schema));
}

void OperationFactory::Init(const IndexPartitionSchemaPtr& schema)
{
    if (schema->GetIndexSchema()->HasPrimaryKeyIndex()) {
        mMainPkType = schema->GetIndexSchema()->GetPrimaryKeyIndexType();
    }

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        mUpdateOperationCreator.reset(new SubDocOperationCreator(schema, CreateUpdateFieldOperationCreator(schema),
                                                                 CreateUpdateFieldOperationCreator(subSchema)));
        mRemoveSubOperationCreator.reset(
            new SubDocOperationCreator(schema, OperationCreatorPtr(), CreateRemoveOperationCreator(subSchema)));
        if (subSchema->GetIndexSchema()->HasPrimaryKeyIndex()) {
            mSubPkType = subSchema->GetIndexSchema()->GetPrimaryKeyIndexType();
            assert(mSubPkType == it_primarykey64 || mSubPkType == it_primarykey128);
        }
    } else {
        mUpdateOperationCreator = CreateUpdateFieldOperationCreator(schema);
    }
    mRemoveOperationCreator = CreateRemoveOperationCreator(schema);
}

bool OperationFactory::CreateOperation(const NormalDocumentPtr& doc, Pool* pool, OperationBase** operation)
{
    DocOperateType opType = doc->GetDocOperateType();
    if (opType == ADD_DOC || opType == DELETE_DOC) {
        return mRemoveOperationCreator->Create(doc, pool, operation);
    }

    if (opType == UPDATE_FIELD) {
        return mUpdateOperationCreator->Create(doc, pool, operation);
    }

    if (opType == DELETE_SUB_DOC && mRemoveSubOperationCreator) {
        return mRemoveSubOperationCreator->Create(doc, pool, operation);
    }

    return false;
}
}} // namespace indexlib::partition
