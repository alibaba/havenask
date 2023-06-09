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
#include "indexlib/partition/segment/segment_sync_item.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SegmentSyncItem);

const size_t SegmentSyncItem::DEFAULT_BUFFER_SIZE = 1024 * 1024 * 2; // 2MB

SegmentSyncItem::SegmentSyncItem(const SegmentInfoSynchronizerPtr& synchronizer, const file_system::FileInfo& fileInfo,
                                 const file_system::DirectoryPtr& sourceSegDir,
                                 const file_system::DirectoryPtr& targetSegDir)
    : mSynchronizer(synchronizer)
    , mFileInfo(fileInfo)
    , mSourceSegDir(sourceSegDir)
    , mTargetSegDir(targetSegDir)
{
}

SegmentSyncItem::~SegmentSyncItem() {}

void SegmentSyncItem::process()
{
    // IE_LOG(INFO, "begin sync path: %s", mFileInfo.filePath.c_str());
    if (mFileInfo.isDirectory()) {
        mTargetSegDir->MakeDirectory(mFileInfo.filePath);
        // IE_LOG(INFO, "end sync path: %s", mFileInfo.filePath.c_str());
        mSynchronizer->TrySyncSegmentInfo(mTargetSegDir);
        return;
    }
    // sync segment
    file_system::FileReaderPtr fileReader = mSourceSegDir->CreateFileReader(
        mFileInfo.filePath, file_system::ReaderOption::NoCache(file_system::FSOT_BUFFERED));
    char* buffer = new char[DEFAULT_BUFFER_SIZE];
    string fileParentDir = util::PathUtil::GetParentDirPath(mFileInfo.filePath);
    mTargetSegDir->MakeDirectory(fileParentDir);
    file_system::FileWriterPtr fileWriter = mTargetSegDir->CreateFileWriter(mFileInfo.filePath);
    size_t leftLen = fileReader->GetLength();
    // std::cout << "file len:" << leftLen << std::endl;
    while (leftLen) {
        size_t readLen = leftLen < DEFAULT_BUFFER_SIZE ? leftLen : DEFAULT_BUFFER_SIZE;
        size_t len = fileReader->Read(buffer, readLen).GetOrThrow();
        fileWriter->Write(buffer, len).GetOrThrow();
        leftLen -= len;
    }
    fileWriter->Close().GetOrThrow();
    delete[] buffer;
    // IE_LOG(INFO, "end sync path: %s", mFileInfo.filePath.c_str());
    // sync counter
    mSynchronizer->TrySyncSegmentInfo(mTargetSegDir);
}
}} // namespace indexlib::partition
