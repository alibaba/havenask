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
#include "indexlib/framework/cleaner/TabletReaderCleaner.h"

#include "autil/TimeUtility.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, TabletReaderCleaner);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

TabletReaderCleaner::TabletReaderCleaner(TabletReaderContainer* tabletReaderContainer,
                                         const std::shared_ptr<indexlib::file_system::IFileSystem>& fileSystem,
                                         const std::string& tabletName)
    : _tabletReaderContainer(tabletReaderContainer)
    , _fileSystem(fileSystem)
    , _tabletName(tabletName)
{
}

Status TabletReaderCleaner::Clean()
{
    size_t currentReaderSize = _tabletReaderContainer->Size();
    size_t currentCleanedSize = 0;
    std::shared_ptr<ITabletReader> reader = _tabletReaderContainer->GetOldestTabletReader();
    while (reader != nullptr && reader.use_count() == 2 && currentCleanedSize < currentReaderSize) {
        _tabletReaderContainer->EvictOldestTabletReader();
        currentCleanedSize++;
        reader = _tabletReaderContainer->GetOldestTabletReader();
    }
    if (currentCleanedSize != 0) {
        TABLET_LOG(INFO, "end clean tablet reader, cleaned reader size[%lu]", currentCleanedSize);
    }

    _fileSystem->CleanCache();

    size_t readerCount = _tabletReaderContainer->Size();
    if (readerCount > TOO_MANY_READER_THRESHOLD) {
        TABLET_LOG(INFO, "too many reader exist, cnt[%lu] tablet[%s]", readerCount, _tabletName.c_str());
    }
    return Status::OK();
}
} // namespace indexlibv2::framework
