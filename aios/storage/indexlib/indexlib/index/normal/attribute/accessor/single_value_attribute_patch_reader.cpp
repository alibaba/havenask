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
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"

#include "indexlib/file_system/file/SnappyCompressFileReader.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;

using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SinglePatchFile);

SinglePatchFile::SinglePatchFile(segmentid_t segmentId, bool patchCompressed, bool supportNull, int64_t recordSize)
    : mFileLength(0)
    , mDocId(INVALID_DOCID)
    , mIsNull(false)
    , mValue(0)
    , mSegmentId(segmentId)
    , mPatchCompressed(patchCompressed)
    , mSupportNull(supportNull)
    , mRecordSize(recordSize)
{
}

SinglePatchFile::~SinglePatchFile() {}

void SinglePatchFile::Open(const DirectoryPtr& directory, const string& fileName)
{
    file_system::FileReaderPtr file = directory->CreateFileReader(fileName, FSOT_BUFFERED);
    assert(file);
    mFormatter.reset(new SingleValueAttributePatchFormatter);
    if (!mPatchCompressed) {
        mFile = file;
        mFileLength = mFile->GetLength();
        mFormatter->InitForRead(mSupportNull, mFile, mRecordSize, mFileLength);
        return;
    }
    SnappyCompressFileReaderPtr compressFileReader(new SnappyCompressFileReader);
    if (!compressFileReader->Init(file)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read compressed patch file failed, file path is: %s",
                             file->DebugString().c_str());
    }
    mFile = compressFileReader;
    mFileLength = compressFileReader->GetUncompressedFileLength();
    mFormatter->InitForRead(mSupportNull, mFile, mRecordSize, mFileLength);
}
}} // namespace indexlib::index
