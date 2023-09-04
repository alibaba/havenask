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
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexConfig.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/MetricReporter.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::config {
class IIndexConfig;
class ANNIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::framework {
class IndexTaskResourceManager;
} // namespace indexlibv2::framework

namespace indexlibv2::index::ann {

class AithetaIndexMerger : public IIndexMerger
{
public:
    AithetaIndexMerger() = default;
    virtual ~AithetaIndexMerger() = default;

    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    void InitMetricReporter(const indexlib::util::MetricProviderPtr& provider);
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;

private:
    std::shared_ptr<indexlib::file_system::Directory>
    PrepareTargetIndexDirectory(const std::shared_ptr<indexlib::file_system::Directory>& targetSegmentDir) const;
    std::shared_ptr<indexlib::file_system::Directory>
    GetAithetaIndexDir(const std::shared_ptr<indexlib::file_system::Directory>& segmentDir) const;

    Status DoMerge(const std::vector<SourceSegment>& srcSegments,
                   std::shared_ptr<indexlib::file_system::Directory>& targetDir,
                   const std::shared_ptr<DocMapper>& docMapper);

private:
    std::shared_ptr<indexlibv2::config::ANNIndexConfig> _indexConfig;
    AithetaIndexConfig _aithetaIndexConfig;
    std::string _indexName;
    std::string _embAttrName;
    std::string _docMapperName;

    MetricReporterPtr _metricReporter;
    MetricPtr _mergeLatencyMetric;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
