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
#include "indexlib/partition/flushed_locator_container.h"

#include <chrono>

using namespace std;
using namespace autil;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, FlushedLocatorContainer);

FlushedLocatorContainer::FlushedLocatorContainer(size_t size) : mSize(size) {}

FlushedLocatorContainer::~FlushedLocatorContainer() {}

void FlushedLocatorContainer::Push(std::shared_future<bool>& future, const document::Locator& locator)
{
    ScopedLock l(mLock);
    if (mLocatorQueue.size() >= mSize) {
        mLocatorQueue.pop_front();
    }
    mLocatorQueue.push_back({future, locator});
}

document::Locator FlushedLocatorContainer::GetLastFlushedLocator()
{
    ScopedLock l(mLock);
    for (auto iter = mLocatorQueue.rbegin(); iter != mLocatorQueue.rend(); ++iter) {
        if (IsReady(*iter) && iter->first.get()) {
            mCachedLocator = iter->second;
            break;
        }
    }
    return mCachedLocator;
}

bool FlushedLocatorContainer::HasFlushingLocator()
{
    ScopedLock l(mLock);
    for (auto iter = mLocatorQueue.rbegin(); iter != mLocatorQueue.rend(); ++iter) {
        if (!IsReady(*iter)) {
            return true;
        }
    }
    return false;
}

void FlushedLocatorContainer::Clear()
{
    ScopedLock l(mLock);
    mLocatorQueue.clear();
}

bool FlushedLocatorContainer::IsReady(const LocatorFuture& locatorFuture) const
{
    const std::shared_future<bool>& f = locatorFuture.first;
    if (!f.valid()) {
        IE_LOG(ERROR, "locatorFuture[%s] is not valid", locatorFuture.second.ToString().c_str());
        return false;
    }

    bool isReady = (f.wait_for(std::chrono::seconds(0)) == std::future_status::ready);
    return isReady;
}
}} // namespace indexlib::partition
