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
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_file.h"

#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/file_system/file/SnappyCompressFileReader.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace fslib;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarNumAttributePatchFile);

VarNumAttributePatchFile::VarNumAttributePatchFile(segmentid_t segmentId, const AttributeConfigPtr& attrConfig)
    : mFileLength(0)
    , mCursor(0)
    , mDocId(INVALID_DOCID)
    , mSegmentId(segmentId)
    , mPatchItemCount(0)
    , mMaxPatchLen(0)
    , mPatchDataLen(0)
    , mFixedValueCount(attrConfig->GetFieldConfig()->GetFixedMultiValueCount())
    , mPatchCompressed(attrConfig->GetCompressType().HasPatchCompress())
{
    if (mFixedValueCount != -1) {
        mPatchDataLen = attrConfig->GetFixLenFieldSize();
    }
}

VarNumAttributePatchFile::~VarNumAttributePatchFile() {}

void VarNumAttributePatchFile::InitPatchFileReader(const DirectoryPtr& directory, const string& fileName)
{
    file_system::FileReaderPtr file = directory->CreateFileReader(fileName, FSOT_BUFFERED);
    if (!mPatchCompressed) {
        mFile = file;
        mFileLength = mFile->GetLength();
        return;
    }
    SnappyCompressFileReaderPtr compressFileReader(new SnappyCompressFileReader);
    if (!compressFileReader->Init(file)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read compressed patch file failed, file path is: %s",
                             file->DebugString().c_str());
    }
    mFile = compressFileReader;
    mFileLength = compressFileReader->GetUncompressedFileLength();
}

void VarNumAttributePatchFile::Open(const DirectoryPtr& directory, const string& fileName)
{
    InitPatchFileReader(directory, fileName);
    if (mFileLength < ((int64_t)sizeof(uint32_t) * 2)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read patch file failed, file path is: %s", mFile->DebugString().c_str());
    }

    size_t beginPos = mFileLength - sizeof(uint32_t) * 2;
    if (mFile->Read((void*)&mPatchItemCount, sizeof(uint32_t), beginPos).GetOrThrow() < (size_t)sizeof(uint32_t)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read patch file failed, file path is: %s", mFile->DebugString().c_str());
    }
    beginPos += sizeof(uint32_t);
    if (mFile->Read((void*)&mMaxPatchLen, sizeof(uint32_t), beginPos).GetOrThrow() < (ssize_t)sizeof(uint32_t)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read patch file failed, file path is: %s", mFile->DebugString().c_str());
    }
}
}} // namespace indexlib::index
