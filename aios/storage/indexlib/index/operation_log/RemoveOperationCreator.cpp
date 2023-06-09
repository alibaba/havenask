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
#include "indexlib/index/operation_log/RemoveOperationCreator.h"

#include "indexlib/index/operation_log/RemoveOperation.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, RemoveOperationCreator);

bool RemoveOperationCreator::Create(const indexlibv2::document::NormalDocument* doc, autil::mem_pool::Pool* pool,
                                    OperationBase** operation)
{
    assert(doc->HasPrimaryKey());
    autil::uint128_t pkHash;
    GetPkHash(doc->GetIndexDocument(), pkHash);
    segmentid_t segmentId = doc->GetSegmentIdBeforeModified();
    auto type = GetPkIndexType();
    auto docInfo = doc->GetDocInfo();
    if (type == it_primarykey64) {
        RemoveOperation<uint64_t>* removeOperation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<uint64_t>, docInfo.timestamp, docInfo.hashId);
        removeOperation->Init(pkHash.value[1], segmentId);
        *operation = removeOperation;
    } else {
        assert(type == it_primarykey128);
        RemoveOperation<autil::uint128_t>* removeOperation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, RemoveOperation<autil::uint128_t>, docInfo.timestamp, docInfo.hashId);
        removeOperation->Init(pkHash, segmentId);
        *operation = removeOperation;
    }

    if (!*operation) {
        AUTIL_LOG(ERROR, "allocate memory fail!");
        return false;
    }
    return true;
}

} // namespace indexlib::index
