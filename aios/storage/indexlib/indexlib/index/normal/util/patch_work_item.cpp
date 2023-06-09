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
#include "indexlib/index/normal/util/patch_work_item.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PatchWorkItem);

PatchWorkItem::PatchWorkItem(const std::string& id, size_t patchItemCount, bool isSub, PatchWorkItemType ite_type)
    : _identifier(id)
    , _patchCount(patchItemCount)
    , _cost(0u)
    , _isSub(isSub)
    , _type(ite_type)
    , _processLimit(std::numeric_limits<size_t>::max())
    , _lastProcessTime(0)
    , _lastProcessCount(0)
{
}

void PatchWorkItem::process()
{
    autil::ScopedLock lock(_lock);
    IE_LOG(INFO, "load patch [%s] begin!", _identifier.c_str());
    int64_t startTime = autil::TimeUtility::currentTimeInMicroSeconds();
    size_t count = 0;
    while (HasNext() && count < _processLimit) {
        count += ProcessNext();
    }
    int64_t endTime = autil::TimeUtility::currentTimeInMicroSeconds();
    _lastProcessTime = endTime - startTime;
    _lastProcessCount = count;
    IE_LOG(INFO, "load patch [%s] finished,  processTimeInMicroSeconds[%ld], processCount[%ld]", _identifier.c_str(),
           _lastProcessTime, _lastProcessCount);
}

int64_t PatchWorkItem::GetLastProcessTime() const
{
    autil::ScopedLock lock(_lock);
    return _lastProcessTime;
}

size_t PatchWorkItem::GetLastProcessCount() const
{
    autil::ScopedLock lock(_lock);
    return _lastProcessCount;
}
}} // namespace indexlib::index
