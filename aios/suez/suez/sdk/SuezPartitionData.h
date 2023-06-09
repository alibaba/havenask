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

#include <functional>

#include "suez/sdk/PartitionId.h"
#include "suez/sdk/SuezPartitionType.h"

namespace suez {

class SuezPartition;

class SuezPartitionData {
public:
    SuezPartitionData(const PartitionId &pid = PartitionId(), SuezPartitionType type = SPT_NONE);
    virtual ~SuezPartitionData();

public:
    const PartitionId &getPartitionId() const;
    SuezPartitionType type() const;

    void setOnRelease(std::function<void()> callback);

    bool operator==(const SuezPartitionData &other) const;
    bool operator!=(const SuezPartitionData &other) const;

private:
    virtual bool detailEqual(const SuezPartitionData &other) const;

protected:
    PartitionId _pid;
    SuezPartitionType _type;
    std::function<void()> _releaseFn;
};

using SuezPartitionDataPtr = std::shared_ptr<SuezPartitionData>;

} // namespace suez
