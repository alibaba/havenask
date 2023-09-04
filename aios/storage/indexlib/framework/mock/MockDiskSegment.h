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
#include "gmock/gmock.h"
#include <memory>

#include "indexlib/base/Status.h"
#include "indexlib/framework/DiskSegment.h"

namespace indexlibv2::framework {
struct SegmentMeta;

class MockDiskSegment : public DiskSegment
{
public:
    // MockDiskSegment() = default;
    explicit MockDiskSegment(const SegmentMeta& segmentMeta) : DiskSegment(segmentMeta) {}
    ~MockDiskSegment() = default;

    MOCK_METHOD(Status, Open, (const std::shared_ptr<MemoryQuotaController>&, DiskSegment::OpenMode), (override));
    MOCK_METHOD(Status, Reopen, (const std::vector<std::shared_ptr<config::ITabletSchema>>&), (override));
    MOCK_METHOD(size_t, EvaluateCurrentMemUsed, (), (override));
    MOCK_METHOD(size_t, EstimateMemUsed, (const std::shared_ptr<config::ITabletSchema>&), (override));
};
} // namespace indexlibv2::framework
