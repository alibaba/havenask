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
#include <mutex>
#include <vector>

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/TabletWriter.h"

namespace indexlibv2::framework {
class TabletReaderContainer;

class TabletMemoryCalculator final
{
public:
    TabletMemoryCalculator(const std::shared_ptr<TabletWriter>& tabletWriter,
                           const std::shared_ptr<TabletReaderContainer>& tabletReaderContainer)
        : _tabletWriter(tabletWriter)
        , _tabletReaderContainer(tabletReaderContainer)
    {
    }
    ~TabletMemoryCalculator() = default;

    size_t GetRtBuiltSegmentsMemsize() const;
    size_t GetRtIndexMemsize() const;
    size_t GetIncIndexMemsize() const;
    size_t GetBuildingSegmentMemsize() const;
    size_t GetDumpingSegmentMemsize() const;
    size_t GetBuildingSegmentDumpExpandMemsize() const;

private:
    mutable std::mutex _mutex;
    std::shared_ptr<TabletWriter> _tabletWriter;
    std::shared_ptr<TabletReaderContainer> _tabletReaderContainer;
};

} // namespace indexlibv2::framework
