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

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

struct SegmentTopologyInfo : autil::legacy::Jsonizable {
public:
    SegmentTopologyInfo();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    indexlibv2::framework::LevelTopology levelTopology;
    uint32_t levelIdx;
    bool isBottomLevel;
    size_t columnCount; // TODO remove?
    size_t columnIdx;
};

DEFINE_SHARED_PTR(SegmentTopologyInfo);
}} // namespace indexlib::index_base
