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
#include "indexlib/base/Status.h"
#include "indexlib/index/operation_log/OperationRedoStrategy.h"

namespace indexlibv2::framework {
class TabletData;
}

namespace indexlib::index {

class DoNothingRedoStrategy : public OperationRedoStrategy
{
public:
    DoNothingRedoStrategy() = default;
    ~DoNothingRedoStrategy() = default;

public:
    Status Init(const indexlibv2::framework::TabletData*, segmentid_t, const std::vector<segmentid_t>&) override;
    bool NeedRedo(segmentid_t, OperationBase*, std::vector<std::pair<docid_t, docid_t>>&) const override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
