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

#include "autil/Log.h"
#include "autil/WorkItem.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace service {
class TopicPartitionSupervisor;
} // namespace service
} // namespace swift

namespace swift {
namespace service {

class UnloadWorkItem : public autil::WorkItem {
public:
    UnloadWorkItem(TopicPartitionSupervisor *tpSupervisor, const protocol::PartitionId &partId);
    ~UnloadWorkItem();

private:
    UnloadWorkItem(const UnloadWorkItem &);
    UnloadWorkItem &operator=(const UnloadWorkItem &);

public:
    void process();

private:
    TopicPartitionSupervisor *_tpSupervisor;
    protocol::PartitionId _partId;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(UnloadWorkItem);

} // namespace service
} // namespace swift
