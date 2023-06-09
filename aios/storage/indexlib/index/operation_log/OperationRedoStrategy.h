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
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {
class TabletData;
}
namespace indexlib::index {
class OperationBase;

class OperationRedoStrategy
{
public:
    OperationRedoStrategy() {}
    virtual ~OperationRedoStrategy() {}

public:
    virtual bool NeedRedo(segmentid_t operationSegment, OperationBase* operation,
                          std::vector<std::pair<docid_t, docid_t>>& targetDocRanges) const = 0;

    virtual Status Init(const indexlibv2::framework::TabletData* tabletData, segmentid_t currentSegmentId,
                        const std::vector<segmentid_t>& targetSegment) = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
