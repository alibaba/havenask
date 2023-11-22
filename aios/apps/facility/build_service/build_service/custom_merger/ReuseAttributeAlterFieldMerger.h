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
#include "build_service/custom_merger/AlterDefaultFieldMerger.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/attribute_data_patcher.h"
#include "indexlib/partition/remote_access/partition_iterator.h"

namespace build_service { namespace custom_merger {

class ReuseAttributeAlterFieldMerger : public AlterDefaultFieldMerger
{
public:
    ReuseAttributeAlterFieldMerger(uint32_t backupId = 0, const std::string& epochId = "");
    ~ReuseAttributeAlterFieldMerger();

private:
    ReuseAttributeAlterFieldMerger(const ReuseAttributeAlterFieldMerger&);
    ReuseAttributeAlterFieldMerger& operator=(const ReuseAttributeAlterFieldMerger&);

protected:
    void FillAttributeValue(const indexlib::partition::PartitionIteratorPtr& partIter, const std::string& attrName,
                            segmentid_t segmentId,
                            const indexlib::partition::AttributeDataPatcherPtr& attrPatcher) override;

public:
    static const std::string MERGER_NAME;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ReuseAttributeAlterFieldMerger);

}} // namespace build_service::custom_merger
