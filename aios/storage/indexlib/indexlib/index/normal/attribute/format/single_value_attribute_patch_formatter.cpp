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
#include "indexlib/index/normal/attribute/format/single_value_attribute_patch_formatter.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SingleValueAttributePatchFormatter);

void SingleValueAttributePatchFormatter::InitForWrite(bool supportNull, file_system::FileWriterPtr output)
{
    mIsSupportNull = supportNull;
    mOutput = output;
}

void SingleValueAttributePatchFormatter::InitForRead(bool supportNull, file_system::FileReaderPtr input,
                                                     int64_t recordSize, int64_t fileLength)
{
    mIsSupportNull = supportNull;
    mInput = input;
    mCursor = 0;
    mFileLength = fileLength;
    if (!supportNull) {
        // not support null, calc item count by length
        mPatchItemCount = mFileLength / (sizeof(docid_t) + recordSize);
        if (mPatchItemCount * (sizeof(docid_t) + recordSize) != mFileLength) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "patch[%s] file length[%lu] is inconsistent",
                                 mInput->DebugString().c_str(), mFileLength);
        }
        return;
    }
    // read item count by tail
    size_t beginPos = fileLength - sizeof(uint32_t);
    if (mInput->Read((void*)&mPatchItemCount, sizeof(uint32_t), beginPos).GetOrThrow() < (size_t)sizeof(uint32_t)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read patch item count from patch file[%s] failed",
                             mInput->DebugString().c_str());
    }
}
}} // namespace indexlib::index
