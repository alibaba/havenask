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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"

namespace indexlib::file_system {
class IDirectory;
}
namespace indexlibv2::config {
class IIndexConfig;
} // namespace indexlibv2::config
namespace indexlibv2::framework {
class Segment;
}
namespace indexlibv2::index {
class AttributeConfig;

class PatchFileFinder : private autil::NoCopyable
{
public:
    PatchFileFinder() = default;
    virtual ~PatchFileFinder() = default;

public:
    Status FindAllPatchFiles(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                             const std::shared_ptr<config::IIndexConfig>& indexConfig,
                             std::map<segmentid_t, PatchFileInfos>* patchInfos) const;

public:
    virtual std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetIndexDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir,
                      const std::shared_ptr<config::IIndexConfig>& indexConfig) const;

protected:
    virtual Status FindPatchFiles(const std::shared_ptr<indexlib::file_system::IDirectory>& indexFieldDir,
                                  segmentid_t srcSegmentId, PatchInfos* patchInfos) const = 0;

    static segmentid_t GetSegmentIdFromStr(const std::string& segmentIdStr);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
