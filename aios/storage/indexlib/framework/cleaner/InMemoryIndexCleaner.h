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

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/TabletReaderContainer.h"

namespace indexlibv2::framework {

class InMemoryIndexCleaner final
{
public:
    InMemoryIndexCleaner(const std::string& tabletName, TabletReaderContainer* tabletReaderContainer,
                         const std::shared_ptr<indexlib::file_system::Directory>& tabletDir, std::mutex* cleanerMutex,
                         bool cleanPhysicalFiles)
        : _tabletName(tabletName)
        , _tabletReaderContainer(tabletReaderContainer)
        , _tabletDir(tabletDir)
        , _cleanerMutex(cleanerMutex)
        , _cleanPhysicalFiles(cleanPhysicalFiles)
    {
    }
    ~InMemoryIndexCleaner() = default;

    Status Clean();

private:
    Status DoClean(const std::shared_ptr<indexlib::file_system::Directory>& dir);
    Status CleanUnusedSegments(const std::shared_ptr<indexlib::file_system::Directory>& dir);
    Status CleanUnusedVersions(const std::shared_ptr<indexlib::file_system::Directory>& dir);
    Status CollectUnusedSegments(const std::shared_ptr<indexlib::file_system::Directory>& dir,
                                 fslib::FileList* segments);

private:
    const std::string _tabletName;
    TabletReaderContainer* _tabletReaderContainer;
    std::shared_ptr<indexlib::file_system::Directory> _tabletDir;
    std::mutex* _cleanerMutex;
    bool _cleanPhysicalFiles;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
