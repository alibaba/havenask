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
#include "indexlib/config/impl/merge_config_impl.h"

#include "autil/StringUtil.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, MergeConfigImpl);

MergeConfigImpl::MergeConfigImpl()
    : checkpointInterval(DEFAULT_CHECKPOINT_INTERVAL)
    , enablePackageFile(false)
    , enableInPlanMultiSegmentParallel(true)
    , enableArchiveFile(false)
    , needCalculateTemperature(false)
{
}

MergeConfigImpl::MergeConfigImpl(const MergeConfigImpl& other)
    : splitSegmentConfig(other.splitSegmentConfig)
    , packageFileTagConfigList(other.packageFileTagConfigList)
    , checkpointInterval(other.checkpointInterval)
    , enablePackageFile(other.enablePackageFile)
    , enableInPlanMultiSegmentParallel(other.enableInPlanMultiSegmentParallel)
    , enableArchiveFile(other.enableArchiveFile)
    , needCalculateTemperature(other.needCalculateTemperature)
{
}

MergeConfigImpl::~MergeConfigImpl() {}

void MergeConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("merged_segment_split_strategy", splitSegmentConfig, splitSegmentConfig);
    json.Jsonize("checkpoint_interval", checkpointInterval, checkpointInterval);
    json.Jsonize("enable_package_file", enablePackageFile, enablePackageFile);
    json.Jsonize("enable_in_plan_multi_segment_parallel", enableInPlanMultiSegmentParallel,
                 enableInPlanMultiSegmentParallel);
    packageFileTagConfigList.Jsonize(json);
    json.Jsonize("enable_archive_file", enableArchiveFile, enableArchiveFile);
    json.Jsonize("need_calculate_temperature", needCalculateTemperature, needCalculateTemperature);
}

void MergeConfigImpl::Check() const {}

void MergeConfigImpl::operator=(const MergeConfigImpl& other)
{
    splitSegmentConfig = other.splitSegmentConfig;
    checkpointInterval = other.checkpointInterval;
    enablePackageFile = other.enablePackageFile;
    packageFileTagConfigList = other.packageFileTagConfigList;
    enableInPlanMultiSegmentParallel = other.enableInPlanMultiSegmentParallel;
    enableArchiveFile = other.enableArchiveFile;
    needCalculateTemperature = other.needCalculateTemperature;
}
}} // namespace indexlib::config
