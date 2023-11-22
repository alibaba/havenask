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
#include <vector>

#include "build_service/common_define.h"
#include "build_service/custom_merger/CustomMergePlan.h"
#include "build_service/custom_merger/CustomMergerImpl.h"
#include "build_service/custom_merger/CustomMergerTaskItem.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/attribute_data_patcher.h"
#include "indexlib/partition/remote_access/partition_iterator.h"
#include "indexlib/partition/remote_access/partition_resource_provider.h"

namespace build_service { namespace custom_merger {

class AlterDefaultFieldMerger : public CustomMergerImpl
{
public:
    AlterDefaultFieldMerger(uint32_t backupId = 0, const std::string& epochId = "");
    ~AlterDefaultFieldMerger();

private:
    AlterDefaultFieldMerger(const AlterDefaultFieldMerger&);
    AlterDefaultFieldMerger& operator=(const AlterDefaultFieldMerger&);

public:
    bool estimateCost(std::vector<CustomMergerTaskItem>& taskItem) override;

protected:
    bool doMergeTask(const CustomMergePlan::TaskItem& taskItem,
                     const indexlib::file_system::DirectoryPtr& dir) override;

    virtual void FillAttributeValue(const indexlib::partition::PartitionIteratorPtr& partIter,
                                    const std::string& attrName, segmentid_t segmentId,
                                    const indexlib::partition::AttributeDataPatcherPtr& attrPatcher);

public:
    static const std::string MERGER_NAME;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AlterDefaultFieldMerger);

}} // namespace build_service::custom_merger
