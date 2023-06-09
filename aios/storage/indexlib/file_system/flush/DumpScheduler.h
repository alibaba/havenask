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

#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/flush/Dumpable.h"

namespace indexlib { namespace file_system {

class DumpScheduler
{
public:
    DumpScheduler() {}
    virtual ~DumpScheduler() {}

public:
    virtual bool Init() noexcept = 0;
    virtual ErrorCode Push(const DumpablePtr& dumpTask) noexcept = 0;
    virtual void WaitFinished() noexcept = 0;
    virtual bool HasDumpTask() noexcept = 0;
    virtual ErrorCode WaitDumpQueueEmpty() noexcept = 0;
};

typedef std::shared_ptr<DumpScheduler> DumpSchedulerPtr;
}} // namespace indexlib::file_system
