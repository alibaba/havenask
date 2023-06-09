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

#include <stdint.h>

namespace indexlibv2::framework {

class OpenOptions
{
public:
    enum BuildMode { STREAM, CONSISTENT_BATCH, INCONSISTENT_BATCH };

public:
    OpenOptions() = default;
    OpenOptions(BuildMode buildMode, int32_t consistentModeBuildThreadCount, int32_t inconsistentModeBuildThreadCount);

    OpenOptions(const OpenOptions& other) = default;
    OpenOptions& operator=(const OpenOptions& other) = default;

public:
    void SetBuildMode(BuildMode buildMode);
    BuildMode GetBuildMode() const;
    static OpenOptions StreamBuild();
    static OpenOptions ConsistentBatchBuild();
    static OpenOptions InconsistentBatchBuild();

    void SetConsistentModeBuildThreadCount(int32_t threadCount);
    int32_t GetConsistentModeBuildThreadCount() const;
    void SetInconsistentModeBuildThreadCount(int32_t threadCount);
    int32_t GetInconsistentModeBuildThreadCount() const;

    void SetUpdateControlFlowOnly(bool updateControlFlowOnly);
    bool GetUpdateControlFlowOnly() const;

private:
    bool _updateControlFlowOnly = false;

    BuildMode _buildMode = STREAM;
    int32_t _consistentModeBuildThreadCount = 0;
    int32_t _inconsistentModeBuildThreadCount = 0;
};

class ReopenOptions
{
public:
    ReopenOptions();
    ReopenOptions(const OpenOptions& openOptions);

    bool IsForceReopen() const;
    void SetForce(bool force);

    OpenOptions GetOpenOptions() const;
    void SetOpenOptions(const OpenOptions& openOptions);

private:
    bool _force = false;

    OpenOptions _openOptions;
};

} // namespace indexlibv2::framework
