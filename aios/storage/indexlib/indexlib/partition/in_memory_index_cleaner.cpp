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
#include "indexlib/partition/in_memory_index_cleaner.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/partition/unused_segment_collector.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemoryIndexCleaner);

InMemoryIndexCleaner::InMemoryIndexCleaner(const ReaderContainerPtr& readerContainer, const DirectoryPtr& directory)
    : mReaderContainer(readerContainer)
    , mDirectory(directory)
{
}

InMemoryIndexCleaner::~InMemoryIndexCleaner() {}

void InMemoryIndexCleaner::Execute()
{
    // TODO(xiuchen) listDir not list all version exist in disk
    CleanUnusedIndex(mDirectory->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, false));
    CleanUnusedIndex(mDirectory->GetDirectory(JOIN_INDEX_PARTITION_DIR_NAME, false));
}

void InMemoryIndexCleaner::CleanUnusedIndex(const DirectoryPtr& directory)
{
    if (!directory) {
        return;
    }
    CleanUnusedSegment(directory);
    CleanUnusedVersion(directory);
}

void InMemoryIndexCleaner::CleanUnusedSegment(const DirectoryPtr& directory)
{
    fslib::FileList segments;
    UnusedSegmentCollector::Collect(mReaderContainer, directory, segments);
    bool isSynced = false;
    for (size_t i = 0; i < segments.size(); ++i) {
        if (!isSynced) {
            auto future = mDirectory->Sync(true /* wait finish */);
            future.wait();
            isSynced = true;
        }
        directory->RemoveDirectory(segments[i]);
    }
}

void InMemoryIndexCleaner::CleanUnusedVersion(const DirectoryPtr& directory)
{
    fslib::FileList fileList;
    VersionLoader::ListVersion(directory, fileList);
    if (fileList.size() <= 1) {
        return;
    }
    bool isSynced = false;
    for (size_t i = 0; i < fileList.size() - 1; ++i) {
        if (!isSynced) {
            auto future = mDirectory->Sync(true /* wait finish */);
            future.wait();
            isSynced = true;
        }
        directory->RemoveFile(fileList[i]);
    }
}
}} // namespace indexlib::partition
