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
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/impl/Segment.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentBuildResource.h"
#include "indexlib/index/ann/aitheta2/util/IndexFields.h"
#include "indexlib/index/ann/aitheta2/util/MetricReporter.h"
#include "indexlib/index/ann/aitheta2/util/PkDataHolder.h"
namespace indexlibv2::index::ann {

static const float kBuildMemoryScalingFactor = 1.1f;

class SegmentBuilder
{
public:
    SegmentBuilder(const AithetaIndexConfig& params, const std::string& indexName, const MetricReporterPtr& reporter)
        : _indexConfig(params)
        , _indexName(indexName)
        , _metricReporter(reporter)
    {
    }
    virtual ~SegmentBuilder() = default;

public:
    virtual bool Init(const SegmentBuildResourcePtr& resource) = 0;
    virtual bool Build(const IndexFields& data) = 0;
    virtual bool Dump(const indexlib::file_system::DirectoryPtr& directory,
                      const std::vector<docid_t>* old2NewDocId = nullptr) = 0;

public:
    virtual size_t EstimateCurrentMemoryUse() = 0;
    virtual size_t EstimateTempMemoryUseInDump() = 0;
    virtual size_t EstimateDumpFileSize() = 0;

protected:
    bool DumpPrimaryKey(PkDataHolder& pkHolder, Segment& segment);

protected:
    AithetaIndexConfig _indexConfig;
    std::string _indexName;
    MetricReporterPtr _metricReporter;
    AiThetaMeta _aiThetaMeta;
    PkDataHolder _pkHolder;
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<SegmentBuilder> SegmentBuilderPtr;

} // namespace indexlibv2::index::ann
