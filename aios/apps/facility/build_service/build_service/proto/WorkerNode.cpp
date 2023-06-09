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
#include "build_service/proto/WorkerNode.h"

using namespace std;

namespace build_service { namespace proto {

bool operator==(const hippo::proto::SlotId& l, const hippo::proto::SlotId& r)
{
    return COMPARE_PB_FIELD1(slaveaddress) && COMPARE_PB_FIELD1(id) && COMPARE_PB_FIELD1(declaretime);
}

bool operator!=(const hippo::proto::SlotId& l, const hippo::proto::SlotId& r) { return !(l == r); }

bool operator==(const hippo::proto::AssignedSlot& l, const hippo::proto::AssignedSlot& r)
{
    if (COMPARE_PB_FIELD1(id) && COMPARE_PB_FIELD1(processstatus_size)) {
        if (l.processstatus_size() > 0) {
            return COMPARE_PB_FIELD1(processstatus(0).pid);
        }
        return true;
    }
    return false;
}

bool operator!=(const hippo::proto::AssignedSlot& l, const hippo::proto::AssignedSlot& r) { return !(l == r); }

}} // namespace build_service::proto
