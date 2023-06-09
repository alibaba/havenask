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
#include "build_service/util/BsMetricTagsHandler.h"

#include "autil/StringUtil.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/util/KmonitorUtil.h"

using namespace std;
using namespace autil;
using namespace kmonitor;
using namespace build_service::proto;

namespace build_service { namespace util {
BS_LOG_SETUP(util, BsMetricTagsHandler);

string BsMetricTagsHandler::SERVICE_NAME_PREFIX = "bs";

BsMetricTagsHandler::BsMetricTagsHandler(const PartitionId& pid) : _pid(pid)
{
    _partition = StringUtil::toString(pid.range().from());
    _partition += "_" + StringUtil::toString(pid.range().to());

    setBuildStep(pid.step());
    if (pid.role() == RoleType::ROLE_MERGER) {
        // set merger buildStep & mergePhase in MergerServiceImpl
        _buildStep = "";
    }

    if (pid.role() == RoleType::ROLE_BUILDER || pid.role() == RoleType::ROLE_MERGER) {
        _clusterName = pid.clusternames(0);
    }
    _generationId = StringUtil::toString(pid.buildid().generationid());

    _roleName = ProtoUtil::toRoleString(pid.role());
    _buildIdAppName = StringUtil::toString(pid.buildid().appname());
}

BsMetricTagsHandler::~BsMetricTagsHandler() {}

void BsMetricTagsHandler::getTags(const std::string& filePath, MetricsTags& tags) const
{
    MetricTagsHandler::getTags(filePath, tags);
    if (!_buildStep.empty()) {
        KmonitorUtil::validateAndAddTag("buildStep", _buildStep, tags);
    }
    if (!_clusterName.empty()) {
        KmonitorUtil::validateAndAddTag("clusterName", _clusterName, tags);
    }
    KmonitorUtil::validateAndAddTag("partition", _partition, tags);
    KmonitorUtil::validateAndAddTag("generation", _generationId, tags);
    KmonitorUtil::validateAndAddTag("roleName", _roleName, tags);
    KmonitorUtil::validateAndAddTag("appName", _buildIdAppName, tags);
    if (!_mergePhase.empty()) {
        KmonitorUtil::validateAndAddTag("mergePhase", _mergePhase, tags);
    }
    BS_LOG(DEBUG, "set metrics tag[%s]", tags.ToString().c_str());
}

void BsMetricTagsHandler::setMergePhase(proto::MergeStep mergeStep)
{
    if (mergeStep == proto::MergeStep::MS_UNKNOWN) {
        _mergePhase = "unknown";
    } else if (mergeStep == proto::MergeStep::MS_BEGIN_MERGE) {
        _mergePhase = "begin";
    } else if (mergeStep == proto::MergeStep::MS_DO_MERGE) {
        _mergePhase = "do";
    } else if (mergeStep == proto::MergeStep::MS_END_MERGE) {
        _mergePhase = "end";
    }
}

void BsMetricTagsHandler::setBuildStep(proto::BuildStep buildStep)
{
    if (buildStep == BuildStep::BUILD_STEP_FULL) {
        _buildStep = "full";
    } else if (buildStep == BuildStep::BUILD_STEP_INC) {
        _buildStep = "inc";
    } else {
        _buildStep = "unknown";
    }
}

}} // namespace build_service::util
