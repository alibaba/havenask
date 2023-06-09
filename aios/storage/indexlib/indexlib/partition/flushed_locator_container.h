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
#ifndef __INDEXLIB_FLUSHED_LOCATOR_CONTAINER_H
#define __INDEXLIB_FLUSHED_LOCATOR_CONTAINER_H

#include <deque>
#include <future>
#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/document/locator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

class FlushedLocatorContainer
{
public:
    FlushedLocatorContainer(size_t size);
    ~FlushedLocatorContainer();

public:
    void Push(std::shared_future<bool>& future, const document::Locator& locator);
    document::Locator GetLastFlushedLocator();
    bool HasFlushingLocator();
    void Clear();

private:
    typedef std::pair<std::shared_future<bool>, document::Locator> LocatorFuture;

private:
    bool IsReady(const LocatorFuture& locatorFuture) const;

private:
    size_t mSize;
    std::deque<LocatorFuture> mLocatorQueue;
    autil::ThreadMutex mLock;

    document::Locator mCachedLocator;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FlushedLocatorContainer);
}} // namespace indexlib::partition

#endif //__INDEXLIB_FLUSHED_LOCATOR_CONTAINER_H
