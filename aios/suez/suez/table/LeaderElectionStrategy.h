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

#include <string>

#include "suez/sdk/PartitionId.h"

namespace suez {

enum LeaderElectionStrategyType
{
    ST_TABLE = 0,
    ST_RANGE = 1,
    ST_WORKER = 2,
    ST_TABLE_IGNORE_VERSION = 3,
};

class LeaderElectionStrategy {
public:
    virtual ~LeaderElectionStrategy();

public:
    virtual uint64_t getKey(const PartitionId &pid) const = 0;
    virtual std::string getPath(const PartitionId &pid) const = 0;
    virtual LeaderElectionStrategyType getType() const = 0;

public:
    static LeaderElectionStrategy *create(const std::string &type);
};

} // namespace suez
