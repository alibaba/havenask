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

#include "indexlib/partition/operation_queue/remove_operation_creator.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/partition/operation_queue/remove_operation.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RemoveOperationCreator);

RemoveOperationCreator::RemoveOperationCreator(const IndexPartitionSchemaPtr& schema) : OperationCreator(schema) {}

RemoveOperationCreator::~RemoveOperationCreator() {}

bool RemoveOperationCreator::Create(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                                    OperationBase** operation)
{
    assert(doc->HasPrimaryKey());
    uint128_t pkHash;
    GetPkHash(doc->GetIndexDocument(), pkHash);
    segmentid_t segmentId = doc->GetSegmentIdBeforeModified();

    if (GetPkIndexType() == it_primarykey64) {
        RemoveOperation<uint64_t>* removeOperation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<uint64_t>, doc->GetTimestamp());
        removeOperation->Init(pkHash.value[1], segmentId);
        *operation = removeOperation;
    } else {
        RemoveOperation<uint128_t>* removeOperation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<autil::uint128_t>, doc->GetTimestamp());
        removeOperation->Init(pkHash, segmentId);
        *operation = removeOperation;
    }

    if (!*operation) {
        IE_LOG(ERROR, "allocate memory fail!");
        return false;
    }
    return true;
}

uint128_t RemoveOperationCreator::GetPkHashFromOperation(OperationBase* operation, InvertedIndexType pkIndexType) const
{
    assert(operation);
    if (pkIndexType == it_primarykey64) {
        RemoveOperation<uint64_t>* typed64Op = static_cast<RemoveOperation<uint64_t>*>(operation);
        return typed64Op->GetPkHash();
    }
    RemoveOperation<uint128_t>* typed128Op = static_cast<RemoveOperation<autil::uint128_t>*>(operation);
    return typed128Op->GetPkHash();
}
}} // namespace indexlib::partition
