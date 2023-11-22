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

#include <stdint.h>
#include <string>

#include "build_service/common_define.h"
#include "build_service/config/OfflineMergeConfig.h"
#include "build_service/custom_merger/CustomMerger.h"
#include "build_service/custom_merger/CustomMergerCreator.h"
#include "build_service/custom_merger/MergeResourceProvider.h"
#include "build_service/task_base/TaskBase.h"
#include "build_service/util/Log.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace build_service { namespace task_base {

class NewFieldMergeTask : public TaskBase
{
public:
    NewFieldMergeTask(indexlib::util::MetricProviderPtr metricProvider, uint32_t backupId, const std::string& epochId);
    ~NewFieldMergeTask();

private:
    NewFieldMergeTask(const NewFieldMergeTask&);
    NewFieldMergeTask& operator=(const NewFieldMergeTask&);

public:
    bool init(const std::string& jobParam) override;
    bool run(uint32_t instanceId, Mode mode);
    int32_t getTargetVersionId() const { return _targetVersionId; }

protected:
    virtual custom_merger::MergeResourceProviderPtr prepareResourceProvider();
    bool prepareNewFieldMergeMeta(const custom_merger::MergeResourceProviderPtr& resourceProvider);
    bool beginMerge();
    bool doMerge(uint32_t instanceId);
    bool endMerge();

private:
    std::string getMergeMetaPath();
    custom_merger::CustomMergerPtr createCustomMerger();
    void mergeIncParallelBuildData(const indexlib::index_base::ParallelBuildInfo& incBuildInfo);
    bool parseTargetDescription();

private:
    indexlib::config::IndexPartitionSchemaPtr _newSchema;
    config::OfflineMergeConfig _offlineMergeConfig;
    custom_merger::CustomMergerCreatorPtr _customMergerCreator;
    custom_merger::CustomMergerPtr _newFieldMerger;
    int32_t _targetVersionId;
    int32_t _alterFieldTargetVersionId;
    int32_t _checkpointVersionId;
    std::string _indexRoot;
    uint32_t _opsId;
    uint32_t _backupId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(NewFieldMergeTask);

}} // namespace build_service::task_base
