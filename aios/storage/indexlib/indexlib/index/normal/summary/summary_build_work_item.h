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

#include <limits>
#include <memory>

#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/util/build_work_item.h"
#include "indexlib/index/summary/config/SummaryConfig.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, DocumentCollector);
DECLARE_REFERENCE_CLASS(index, SummaryWriter);

namespace indexlib { namespace index::legacy {

// User of work item should ensure lifetime of summary writer and data out live the work item.
// Summary data format does not support split into multiple build work items easily(as of 2021-10-27). Thus one
// workitem/thread handles all summary build.
// SerializedSummaryDocument format:
// [VarInt32 len1][payload1][VarInt32 len2][payload2][VarInt32 len3][payload3]...
class SummaryBuildWorkItem : public legacy::BuildWorkItem
{
public:
    SummaryBuildWorkItem(index::SummaryWriter* summaryWriter, const document::DocumentCollectorPtr& docCollector,
                         bool isSub);
    ~SummaryBuildWorkItem() {};

public:
    void doProcess() override;

private:
    index::SummaryWriter* _summaryWriter;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::index::legacy
