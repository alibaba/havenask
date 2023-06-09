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
#include "indexlib/merger/segment_metric_update_work_item.h"

using namespace std;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, SegmentMetricUpdateWorkItem);

SegmentMetricUpdateWorkItem::~SegmentMetricUpdateWorkItem() {}

void SegmentMetricUpdateWorkItem::Process()
{
    mUpdater->UpdateForCaculator(mCheckpointDir, mFenceContext);
    mUpdater->ReportMetrics();
}

void SegmentMetricUpdateWorkItem::Destroy() {}

int64_t SegmentMetricUpdateWorkItem::GetRequiredResource() const { return 0; }
}} // namespace indexlib::merger
