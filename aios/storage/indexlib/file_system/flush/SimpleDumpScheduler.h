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

#include "autil/Log.h"
#include "indexlib/file_system/flush/DumpScheduler.h"
#include "indexlib/file_system/flush/Dumpable.h"

namespace indexlib { namespace file_system {

class SimpleDumpScheduler : public DumpScheduler
{
public:
    SimpleDumpScheduler() noexcept;
    ~SimpleDumpScheduler() noexcept;

public:
    bool Init() noexcept override { return true; }
    ErrorCode Push(const DumpablePtr& dumpTask) noexcept override;
    void WaitFinished() noexcept override;
    bool HasDumpTask() noexcept override { return false; }
    ErrorCode WaitDumpQueueEmpty() noexcept override { return FSEC_OK; }

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SimpleDumpScheduler> SimpleDumpSchedulerPtr;
}} // namespace indexlib::file_system
