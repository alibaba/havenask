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
#include "indexlib/framework/SegmentMeta.h"

#include "indexlib/base/Constant.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMetrics.h"

namespace indexlibv2::framework {
SegmentMeta::SegmentMeta() : SegmentMeta(INVALID_SEGMENTID) {}

SegmentMeta::SegmentMeta(segmentid_t segId)
    : segmentId(segId)
    , segmentInfo(std::make_shared<SegmentInfo>())
    , segmentMetrics(std::make_shared<indexlib::framework::SegmentMetrics>())
    , lifecycle("")
{
}

} // namespace indexlibv2::framework
