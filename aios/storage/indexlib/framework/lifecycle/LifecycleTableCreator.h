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
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/framework/Version.h"

namespace indexlib::file_system {
class LifecycleTable;
}

namespace indexlibv2::framework {
class SegmentDescriptions;
class SegmentStatistics;

class LifecycleTableCreator
{
public:
    LifecycleTableCreator() = delete;
    ~LifecycleTableCreator() = delete;

public:
    static std::shared_ptr<indexlib::file_system::LifecycleTable>
    CreateLifecycleTable(const indexlibv2::framework::Version& version,
                         const indexlib::file_system::LifecycleConfig& lifecycleConfig);

    static std::string CalculateLifecycle(const SegmentStatistics& segmentStatistic,
                                          const indexlib::file_system::LifecycleConfig& lifecycleConfig);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
