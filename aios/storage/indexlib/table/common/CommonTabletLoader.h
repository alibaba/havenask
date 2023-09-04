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

#include <any>
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "indexlib/framework/TabletLoader.h"
#include "indexlib/framework/Version.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlib::framework {
struct SegmentMeta;
class Segment;
class Version;
} // namespace indexlib::framework

namespace indexlibv2::table {

class CommonTabletLoader : public framework::TabletLoader
{
public:
    using Segments = std::vector<std::shared_ptr<framework::Segment>>;
    explicit CommonTabletLoader(std::string fenceName) : _fenceName(std::move(fenceName)), _dropRt(false) {}

public:
    std::pair<Status, std::unique_ptr<framework::TabletData>>
    FinalLoad(const framework::TabletData& currentTabletData) override;

protected:
    Status DoPreLoad(const framework::TabletData& lastTabletData, Segments newOnDiskVersionSegments,
                     const framework::Version& newOnDiskVersion) override;

protected:
    std::string _fenceName;
    framework::Version _newVersion;
    std::set<segmentid_t> _segmentIdsOnPreload;
    Segments _newSegments;
    bool _dropRt;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
