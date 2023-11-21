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

namespace indexlib { namespace partition {

struct OperationCursor {
    segmentid_t segId;
    int32_t pos;
    OperationCursor(segmentid_t segmentId = INVALID_SEGMENTID, int32_t position = -1) : segId(segmentId), pos(position)
    {
    }

    bool operator<(const OperationCursor& other) const
    {
        if (segId < other.segId) {
            return true;
        }
        if (segId == other.segId) {
            return pos < other.pos;
        }
        return false;
    }

    bool operator==(const OperationCursor& other) const { return (segId == other.segId) && (pos == other.pos); }

    bool operator>=(const OperationCursor& other) const { return !(*this < other); }
};
}} // namespace indexlib::partition
