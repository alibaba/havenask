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
#include "indexlib/framework/cleaner/ResourceCleaner.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, ResourceCleaner);

ResourceCleaner::ResourceCleaner(TabletReaderContainer* tabletReaderContainer,
                                 const std::shared_ptr<indexlib::file_system::Directory>& rootDirectory,
                                 const std::string& tabletName, bool cleanPhysicalFiles, std::mutex* cleanerMutex,
                                 int64_t expectedIntervalMs)

{
    _expectedIntervalUs = autil::TimeUtility::ms2us(expectedIntervalMs);
    _lastRunTimestamp = autil::TimeUtility::currentTimeInMicroSeconds();
    _tabletName = tabletName;
    assert(tabletReaderContainer != nullptr);

    _tabletReaderCleaner =
        std::make_unique<TabletReaderCleaner>(tabletReaderContainer, rootDirectory->GetFileSystem(), tabletName);
    _inMemoryIndexCleaner = std::make_unique<InMemoryIndexCleaner>(tabletName, tabletReaderContainer, rootDirectory,
                                                                   cleanerMutex, cleanPhysicalFiles);
}

void ResourceCleaner::Run()
{
    int64_t currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
    int64_t cleanInterval = currentTime - _lastRunTimestamp;
    Status status = _tabletReaderCleaner->Clean();
    if (cleanInterval > _expectedIntervalUs * 2) {
        AUTIL_LOG(INFO, "clean resource interval more than [%ld] seconds, tablet:[%s]", cleanInterval / 1000 / 1000,
                  _tabletName.c_str());
    }

    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "Run tablet reader cleaner failed.");
    }

    status = _inMemoryIndexCleaner->Clean();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "Run memory index cleaner failed.");
    }

    _lastRunTimestamp = autil::TimeUtility::currentTimeInMicroSeconds();
    auto executeTimeUsed = _lastRunTimestamp - currentTime;
    if (executeTimeUsed > CLEAN_TIMEUSED_THRESHOLD) {
        AUTIL_LOG(INFO, "execute clean function over [%ld] microseconds, tablet[%s].", executeTimeUsed,
                  _tabletName.c_str());
    }
}

} // namespace indexlibv2::framework
