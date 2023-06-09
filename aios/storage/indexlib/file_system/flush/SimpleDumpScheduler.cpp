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
#include "indexlib/file_system/flush/SimpleDumpScheduler.h"

#include <iosfwd>
#include <memory>

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SimpleDumpScheduler);

SimpleDumpScheduler::SimpleDumpScheduler() noexcept {}

SimpleDumpScheduler::~SimpleDumpScheduler() noexcept {}

ErrorCode SimpleDumpScheduler::Push(const DumpablePtr& dumpTask) noexcept
{
    RETURN_IF_FS_EXCEPTION(dumpTask->Dump(), "Dump failed");
    return FSEC_OK;
}

void SimpleDumpScheduler::WaitFinished() noexcept { return; }
}} // namespace indexlib::file_system
