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
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/TabletLoader.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/table/common/CommonTabletLoader.h"

namespace indexlib::file_system {
class Directory;
}
namespace indexlib::index {
class PrimaryKeyIndexReader;
struct OperationCursor;
class OperationLogReplayer;
} // namespace indexlib::index

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::index {
class IIndexReader;
} // namespace indexlibv2::index

namespace indexlibv2::framework {
class Segment;
class ResourceMap;
} // namespace indexlibv2::framework

namespace indexlibv2::table {
class NormalTabletModifier;

class NormalTabletLoader : public CommonTabletLoader
{
public:
    using Segments = std::vector<std::shared_ptr<framework::Segment>>;

    explicit NormalTabletLoader(const std::string& fenceName, bool reopenDataConsistent, bool loadIndexForCheck);
    ~NormalTabletLoader() = default;

    Status DoPreLoad(const framework::TabletData& lastTabletData, Segments newOnDiskVersionSegments,
                     const framework::Version& newOnDiskVersion) override;
    std::pair<Status, std::unique_ptr<framework::TabletData>>
    FinalLoad(const framework::TabletData& currentTabletData) override;

private:
    size_t EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema,
                           const std::vector<framework::Segment*>& segments) override;
    using RedoParam =
        std::tuple<Status, std::unique_ptr<indexlib::index::OperationLogReplayer>,
                   std::unique_ptr<NormalTabletModifier>, std::unique_ptr<indexlib::index::PrimaryKeyIndexReader>>;
    RedoParam CreateRedoParameters(const framework::TabletData& tabletData) const;
    Status PatchAndRedo(const framework::Version& newOnDiskVersion, Segments newOnDiskVersionSegments,
                        const framework::TabletData& lastTabletData, const framework::TabletData& newTabletData);
    Status RemoveObsoleteRtDocs(const framework::Locator& versionLocator,
                                const std::vector<std::pair<std::shared_ptr<framework::Segment>, docid_t>>& segments,
                                const framework::TabletData& newTabletData);
    std::pair<Status, bool> IsRtFullyFasterThanInc(const framework::TabletData& lastTabletData,
                                                   const framework::Version& newOnDiskVersion);

private:
    bool _reopenDataConsistent = false;
    bool _loadIndexForCheck = false;
    std::vector<segmentid_t> _targetRedoSegments;
    std::set<segmentid_t> _segmentIdsOnPreload;
    std::shared_ptr<indexlib::index::OperationCursor> _opCursor;
    std::shared_ptr<framework::ResourceMap> _resourceMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
