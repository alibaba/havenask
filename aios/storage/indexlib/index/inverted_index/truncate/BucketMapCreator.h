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

#include "autil/ThreadPool.h"
#include "indexlib/index/inverted_index/config/TruncateOptionConfig.h"
#include "indexlib/index/inverted_index/config/TruncateProfile.h"
#include "indexlib/index/inverted_index/truncate/BucketMap.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::index {
class DocMapper;
}

namespace indexlib::index {

class BucketMapCreator
{
public:
    BucketMapCreator() = default;
    ~BucketMapCreator() = default;

public:
    static std::pair<Status, BucketMaps>
    CreateBucketMaps(const std::shared_ptr<indexlibv2::config::TruncateOptionConfig>& truncOptionConfig,
                     const std::shared_ptr<indexlibv2::config::TabletSchema>& tabletSchema,
                     const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
                     TruncateAttributeReaderCreator* attrReaderCreator, int64_t segmentMergePlanIdx);

    static std::string GetBucketMapName(size_t segmentMergePlanIdx, const std::string& truncateProfileName);

private:
    static bool NeedCreateBucketMap(const std::shared_ptr<indexlibv2::config::TruncateProfile>& truncateProfile,
                                    const std::shared_ptr<indexlibv2::config::TruncateStrategy>& truncateStrategy,
                                    const std::shared_ptr<indexlibv2::config::TabletSchema>& tabletSchema);
    static std::shared_ptr<autil::ThreadPool> CreateBucketMapThreadPool(uint32_t truncateProfileSize);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
