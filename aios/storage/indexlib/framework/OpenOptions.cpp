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
#include "indexlib/framework/OpenOptions.h"

#include "autil/Log.h"

namespace indexlibv2::framework {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlibv2.framework, ReopenOptions);

OpenOptions::OpenOptions(BuildMode buildMode, int32_t consistentModeBuildThreadCount,
                         int32_t inconsistentModeBuildThreadCount)
{
    _buildMode = buildMode;
    _consistentModeBuildThreadCount = consistentModeBuildThreadCount;
    _inconsistentModeBuildThreadCount = inconsistentModeBuildThreadCount;
}

void OpenOptions::SetBuildMode(BuildMode buildMode) { _buildMode = buildMode; }
OpenOptions::BuildMode OpenOptions::GetBuildMode() const { return _buildMode; }
OpenOptions OpenOptions::StreamBuild()
{
    OpenOptions options;
    options.SetBuildMode(STREAM);
    return options;
}
OpenOptions OpenOptions::ConsistentBatchBuild()
{
    OpenOptions options;
    options.SetBuildMode(CONSISTENT_BATCH);
    return options;
}
OpenOptions OpenOptions::InconsistentBatchBuild()
{
    OpenOptions options;
    options.SetBuildMode(INCONSISTENT_BATCH);
    return options;
}

void OpenOptions::SetConsistentModeBuildThreadCount(int32_t threadCount)
{
    _consistentModeBuildThreadCount = threadCount;
}
int32_t OpenOptions::GetConsistentModeBuildThreadCount() const { return _consistentModeBuildThreadCount; }
void OpenOptions::SetInconsistentModeBuildThreadCount(int32_t threadCount)
{
    _inconsistentModeBuildThreadCount = threadCount;
}
int32_t OpenOptions::GetInconsistentModeBuildThreadCount() const { return _inconsistentModeBuildThreadCount; }

void OpenOptions::SetUpdateControlFlowOnly(bool updateControlFlowOnly)
{
    _updateControlFlowOnly = updateControlFlowOnly;
}
bool OpenOptions::GetUpdateControlFlowOnly() const { return _updateControlFlowOnly; }

ReopenOptions::ReopenOptions() { _force = false; }
ReopenOptions::ReopenOptions(const OpenOptions& openOptions)
{
    _openOptions = openOptions;
    _force = false;
}

void ReopenOptions::SetForce(bool force) { _force = force; }
bool ReopenOptions::IsForceReopen() const { return _force; }

OpenOptions ReopenOptions::GetOpenOptions() const { return _openOptions; }
void ReopenOptions::SetOpenOptions(const OpenOptions& openOptions) { _openOptions = openOptions; }

} // namespace indexlibv2::framework
