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
#pragma once

#include <deque>
#include <map>
#include <set>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/admin/ErrorHandlerTemplate.h"
#include "swift/common/Common.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/MessageComparator.h" // IWYU pragma: keep

namespace swift {
namespace admin {

typedef ErrorHandlerTemplate<protocol::PartitionId, protocol::PartitionError, protocol::PartitionErrorResponse>
    PartitionErrorHandlerBase;

class PartitionErrorHandler : public PartitionErrorHandlerBase {
private:
    typedef std::map<protocol::PartitionId, protocol::ErrorCode> PartitionErrorMap;
    typedef std::deque<protocol::PartitionError> PartitionErrorQueue;
    typedef std::set<protocol::ErrorCode> ErrorCodeSet;

public:
    PartitionErrorHandler();
    ~PartitionErrorHandler();

private:
    PartitionErrorHandler(const PartitionErrorHandler &);
    PartitionErrorHandler &operator=(const PartitionErrorHandler &);

public:
    void addError(const protocol::PartitionId &partitionId,
                  protocol::ErrorCode errorCode,
                  int64_t errTime,
                  const std::string &errMsg);

private:
    friend class PartitionErrorHandlerTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(PartitionErrorHandler);

} // namespace admin
} // namespace swift
