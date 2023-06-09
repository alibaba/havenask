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
#include "indexlib/partition/segment/async_segment_dumper.h"

#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/segment/dump_segment_container.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, AsyncSegmentDumper);

AsyncSegmentDumper::AsyncSegmentDumper(OnlinePartition* onlinePartition, autil::RecursiveThreadMutex& cleanerLock)
    : mOnlinePartition(onlinePartition)
    , mCleanerLock(cleanerLock)
    , mIsDumping(false)
    , mIsRunning(true)
{
    assert(onlinePartition);
}

AsyncSegmentDumper::~AsyncSegmentDumper() { mIsRunning = false; }

void AsyncSegmentDumper::TriggerDumpIfNecessary()
{
    if (mIsDumping) {
        return;
    }

    ScopedLock lock(mLock);
    mDumpSegmentThread.reset();
    const DumpSegmentContainerPtr& dumpSegContainer = mOnlinePartition->GetDumpSegmentContainer();
    if (!dumpSegContainer || dumpSegContainer->GetUnDumpedSegmentSize() == 0) {
        // no need dump
        return;
    }

    mDumpSegmentThread =
        autil::Thread::createThread(std::bind(&AsyncSegmentDumper::DumpSegmentThread, this), "indexAsyncFlush");
    if (!mDumpSegmentThread) {
        IE_LOG(ERROR, "create dump segment thread failed, retry later");
    } else {
        mIsDumping = true;
    }
}

void AsyncSegmentDumper::DumpSegmentThread()
{
    while (mIsRunning) {
        if (mCleanerLock.trylock() != 0) {
            usleep(500 * 1000); // 500ms
            continue;
        }
        mOnlinePartition->FlushDumpSegmentInContainer();
        mCleanerLock.unlock();
        break;
    }
    mIsDumping = false;
}
}} // namespace indexlib::partition
