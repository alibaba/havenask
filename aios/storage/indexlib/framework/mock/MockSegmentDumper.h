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
#include <assert.h>
#include <memory>
#include <vector>

#include "indexlib/framework/SegmentDumper.h"

namespace indexlibv2::framework {

class Version;

class MockSegmentDumper : public SegmentDumper
{
public:
    MockSegmentDumper(const std::shared_ptr<MemSegment>& segment, int64_t dumpExpandMemSize,
                      std::shared_ptr<kmonitor::MetricsReporter> metricsReporter)
        : SegmentDumper("mock", segment, dumpExpandMemSize, metricsReporter)
    {
    }
    ~MockSegmentDumper() = default;
    MOCK_METHOD(Status, StoreSegmentInfo, (), (override));
};

} // namespace indexlibv2::framework
