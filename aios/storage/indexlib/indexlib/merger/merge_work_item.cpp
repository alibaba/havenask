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
#include "indexlib/merger/merge_work_item.h"

#include <assert.h>
#include <iosfwd>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/fslib/IoConfig.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeWorkItem);

void MergeWorkItem::Process()
{
    assert(mMergeFileSystem);
    if (mMergeFileSystem->HasCheckpoint(mCheckPointName)) {
        IE_LOG(INFO, "mergeWorkItem [%s] has already been done!", mIdentifier.c_str());
        ReportMetrics();
        return;
    }

    if (mItemMetrics) {
        mItemMetrics->SetStartTimestamp(autil::TimeUtility::currentTimeInMicroSeconds());
    }

    DoProcess();
    if (!mCheckPointName.empty()) {
        mMergeFileSystem->MakeCheckpoint(mCheckPointName);
    }
}
}} // namespace indexlib::merger
