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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/table/normal_table/NormalTabletLoader.h"

namespace indexlibv2::table {
class OrcTabletLoader : public NormalTabletLoader
{
public:
    explicit OrcTabletLoader(const std::string& fenceName, bool reopenDataConsistent, bool loadIndexForCheck);
    ~OrcTabletLoader();

public:
    Status DoPreLoad(const framework::TabletData& lastTabletData,
                     std::vector<std::shared_ptr<framework::Segment>> newOnDiskVersionSegments,
                     const framework::Version& newOnDiskVersion) override;
    std::pair<Status, std::unique_ptr<framework::TabletData>>
    FinalLoad(const framework::TabletData& currentTabletData) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
