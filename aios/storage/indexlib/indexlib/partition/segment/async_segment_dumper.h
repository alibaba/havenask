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
#ifndef __INDEXLIB_ASYNC_SEGMENT_DUMPER_H
#define __INDEXLIB_ASYNC_SEGMENT_DUMPER_H

#include <memory>

#include "autil/Lock.h"
#include "autil/Thread.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(partition, OnlinePartition);

namespace indexlib { namespace partition {

class AsyncSegmentDumper
{
public:
    AsyncSegmentDumper(OnlinePartition* onlinePartition, autil::RecursiveThreadMutex& cleanerLock);
    ~AsyncSegmentDumper();

public:
    void TriggerDumpIfNecessary();

private:
    void DumpSegmentThread();

private:
    OnlinePartition* mOnlinePartition;
    autil::ThreadPtr mDumpSegmentThread;
    autil::ThreadMutex mLock;
    autil::RecursiveThreadMutex& mCleanerLock;
    volatile bool mIsDumping;
    volatile bool mIsRunning;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AsyncSegmentDumper);
}} // namespace indexlib::partition

#endif //__INDEXLIB_ASYNC_SEGMENT_DUMPER_H
