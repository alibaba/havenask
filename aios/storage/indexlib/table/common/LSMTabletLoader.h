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
#include "indexlib/framework/Version.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/table/common/CommonTabletLoader.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::framework {
class Segment;
} // namespace indexlibv2::framework

namespace indexlibv2::table {
class LSMTabletLoader : public CommonTabletLoader
{
public:
    using Segments = std::vector<std::shared_ptr<framework::Segment>>;

    explicit LSMTabletLoader(const std::string& fenceName);
    ~LSMTabletLoader() = default;

    std::pair<Status, std::unique_ptr<framework::TabletData>>
    FinalLoad(const framework::TabletData& currentTabletData) override;

protected:
    Status DoPreLoad(const framework::TabletData& lastTabletData, Segments newOnDiskVersionSegments,
                     const framework::Version& newOnDiskVersion) override;

private:
    size_t EstimateMemUsed(const std::shared_ptr<config::TabletSchema>& schema,
                           const std::vector<framework::Segment*>& segments) override;

private:
    friend class LSMTabletLoaderTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
