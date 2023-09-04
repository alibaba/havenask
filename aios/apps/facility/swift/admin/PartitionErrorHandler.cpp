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
#include "swift/admin/PartitionErrorHandler.h"

#include "autil/TimeUtility.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, PartitionErrorHandler);
using namespace swift::protocol;
using namespace autil;

PartitionErrorHandler::PartitionErrorHandler() {}

PartitionErrorHandler::~PartitionErrorHandler() {}

void PartitionErrorHandler::addError(const PartitionId &partitionId,
                                     ErrorCode errorCode,
                                     int64_t errTime,
                                     const std::string &errMsg) {
    PartitionError partitionError;
    *(partitionError.mutable_partitionid()) = partitionId;
    partitionError.set_errcode(errorCode);
    partitionError.set_errtime(errTime);
    partitionError.set_errmsg(errMsg);

    if (PartitionErrorHandlerBase::addError(partitionId, partitionError)) {
        AUTIL_LOG(ERROR, "Get Error:%s", partitionError.ShortDebugString().c_str());
    }
}

} // namespace admin
} // namespace swift
