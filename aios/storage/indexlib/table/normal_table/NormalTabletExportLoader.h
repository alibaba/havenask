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
#include "indexlib/table/common/CommonTabletLoader.h"

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::framework {
class Segment;
class TabletData;
} // namespace indexlibv2::framework

namespace indexlib::file_system {
class IDirectory;
}
namespace indexlib::index {
class PrimaryKeyIndexReader;
class OperationLogReplayer;
} // namespace indexlib::index

namespace indexlibv2::table {
class NormalTabletModifier;
}

namespace indexlib::table {

class NormalTabletExportLoader : public indexlibv2::table::CommonTabletLoader
{
public:
    NormalTabletExportLoader(const std::string& fenceName, const std::string& workPath,
                             const std::optional<std::set<segmentid_t>>& targetSegmentIds);
    ~NormalTabletExportLoader() = default;

public:
    // Given a tabletData as input, return the loaded tabetData
    std::pair<Status, std::unique_ptr<indexlibv2::framework::TabletData>>
    ProcessTabletData(const std::shared_ptr<indexlibv2::framework::TabletData>& sourceTabletData);

public:
    using Segments = std::vector<std::shared_ptr<indexlibv2::framework::Segment>>;
    Status DoPreLoad(const indexlibv2::framework::TabletData& lastTabletData, Segments newOnDiskVersionSegments,
                     const indexlibv2::framework::Version& newOnDiskVersion) override;

    std::pair<Status, std::unique_ptr<indexlibv2::framework::TabletData>>
    FinalLoad(const indexlibv2::framework::TabletData& currentTabletData) override;

private:
    using RedoParam = std::tuple<Status, std::unique_ptr<index::OperationLogReplayer>,
                                 std::unique_ptr<indexlibv2::table::NormalTabletModifier>,
                                 std::unique_ptr<index::PrimaryKeyIndexReader>>;
    RedoParam CreateRedoParameters(const indexlibv2::framework::TabletData& tabletData) const;

private:
    std::unique_ptr<indexlibv2::framework::TabletData> _tabletData;
    std::shared_ptr<file_system::IDirectory> _op2PatchDir;
    std::optional<std::set<segmentid_t>> _targetSegmentIds;
    std::string _workPath;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::table
