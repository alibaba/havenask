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
#include "indexlib/framework/ITabletImporter.h"
#include "indexlib/framework/ImportOptions.h"
#include "indexlib/framework/Version.h"

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlibv2::table {

class Fence;

class CommonVersionImporter : public framework::ITabletImporter
{
public:
    static std::string NOT_SUPPORT;
    static std::string KEEP_SEGMENT_IGNORE_LOCATOR;
    static std::string KEEP_SEGMENT_OVERWRITE_LOCATOR;

    static std::string COMMON_TYPE;

    CommonVersionImporter() = default;
    ~CommonVersionImporter() = default;

    Status Check(const std::vector<framework::Version>& versions, const framework::Version* baseVersion,
                 const framework::ImportOptions& options, std::vector<framework::Version>* validVersions) override;
    Status Import(const std::vector<framework::Version>& versions, const framework::Fence* fence,
                  const framework::ImportOptions& options, framework::Version* baseVersion) override;

private:
    static std::pair<Status, framework::Locator>
    GetSegmentLocator(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir);

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
