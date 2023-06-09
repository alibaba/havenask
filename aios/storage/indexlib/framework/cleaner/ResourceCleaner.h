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
#include <memory>
#include <mutex>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/framework/cleaner/InMemoryIndexCleaner.h"
#include "indexlib/framework/cleaner/TabletReaderCleaner.h"

namespace indexlibv2::framework {
class TabletReaderContainer;

class ResourceCleaner final
{
public:
    ResourceCleaner(TabletReaderContainer* tabletReaderContainer,
                    const std::shared_ptr<indexlib::file_system::Directory>& rootDirectory,
                    const std::string& tabletName, bool cleanPhysicalFiles, std::mutex* cleanerMutex,
                    int64_t expectedIntervalMs);
    ~ResourceCleaner() = default;

    void Run();

private:
    static constexpr int64_t CLEAN_TIMEUSED_THRESHOLD = 10 * 1000 * 1000; // 10s

    int64_t _expectedIntervalUs;
    int64_t _lastRunTimestamp;
    std::string _tabletName;
    std::unique_ptr<TabletReaderCleaner> _tabletReaderCleaner;
    std::unique_ptr<InMemoryIndexCleaner> _inMemoryIndexCleaner;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
