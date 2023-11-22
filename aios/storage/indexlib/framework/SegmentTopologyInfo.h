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

#include <memory>
#include <stddef.h>
#include <stdint.h>

#include "autil/legacy/jsonizable.h"
#include "indexlib/framework/LevelInfo.h"

namespace indexlibv2::framework {

struct SegmentTopologyInfo : autil::legacy::Jsonizable {
public:
    SegmentTopologyInfo();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    LevelTopology levelTopology;
    uint32_t levelIdx;
    bool isBottomLevel;
    size_t shardCount; // TODO remove?
    size_t shardIdx;
};

typedef std::shared_ptr<SegmentTopologyInfo> SegmentTopologyInfoPtr;

} // namespace indexlibv2::framework
