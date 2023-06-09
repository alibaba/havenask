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
#include "indexlib/partition/partition_reader_cleaner.h"

#include "autil/TimeUtility.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/partition/reader_container.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionReaderCleaner);

PartitionReaderCleaner::PartitionReaderCleaner(const ReaderContainerPtr& readerContainer,
                                               const IFileSystemPtr& fileSystem, RecursiveThreadMutex& dataLock,
                                               const string& partitionName)
    : mReaderContainer(readerContainer)
    , mFileSystem(fileSystem)
    , mDataLock(dataLock)
    , mPartitionName(partitionName)
    , mLastTs(0)
{
    assert(readerContainer);
    mLastTs = TimeUtility::currentTime();
}

PartitionReaderCleaner::~PartitionReaderCleaner() {}

void PartitionReaderCleaner::Execute()
{
    int64_t currentTime = TimeUtility::currentTime();
    if (currentTime - mLastTs > 10 * 1000 * 1000) { // 10s
        IE_LOG(INFO, "[%s] Execute() timestamp gap over 10 seconds [%ld]", mPartitionName.c_str(),
               currentTime - mLastTs);
    }

    size_t currentReaderSize = mReaderContainer->Size();
    size_t currentCleanedSize = 0;
    IndexPartitionReaderPtr reader = mReaderContainer->GetOldestReader();
    while (reader && reader.use_count() == 2 && currentCleanedSize < currentReaderSize) {
        IE_LOG(INFO, "[%s] Find target reader to clean", mPartitionName.c_str());
        mReaderContainer->EvictOldestReader();
        ScopedLock readerLock(mDataLock);
        currentCleanedSize++;
        IE_LOG(INFO, "[%s] Begin clean reader", mPartitionName.c_str());
        versionid_t versionId = reader->GetVersion().GetVersionId();
        reader = mReaderContainer->GetOldestReader();
        IE_LOG(INFO, "[%s] End clean reader[%d]", mPartitionName.c_str(), versionId);
    }
    mFileSystem->CleanCache();

    size_t readerCount = mReaderContainer->Size();
    if (readerCount > 10) {
        IE_LOG(INFO, "[%s] readerContainer size[%lu] over 10!", mPartitionName.c_str(), readerCount);
    }

    mLastTs = TimeUtility::currentTime();
    if (mLastTs - currentTime > 10 * 1000 * 1000) // 10s
    {
        IE_LOG(INFO, "[%s] run clean logic over 10 seconds [%ld]", mPartitionName.c_str(), mLastTs - currentTime);
    }
}
}} // namespace indexlib::partition
