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
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, DocumentCollector);
DECLARE_REFERENCE_CLASS(index, SourceWriter);

namespace indexlib { namespace index::legacy {

// User of work item should ensure lifetime of source writer and data out live the work item.
// TODO: Split source build work item into multiple work items.
class SourceBuildWorkItem : public legacy::BuildWorkItem
{
public:
    SourceBuildWorkItem(index::SourceWriter* sourceWriter, const document::DocumentCollectorPtr& docCollector,
                        bool isSub);
    ~SourceBuildWorkItem() {};

public:
    void doProcess() override;

private:
    index::SourceWriter* _sourceWriter;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::index::legacy
