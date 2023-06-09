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
#include "indexlib/index/data_structure/var_len_offset_reader.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarLenOffsetReader);

VarLenOffsetReader::VarLenOffsetReader(const VarLenDataParam& param)
    : mParam(param)
    , mCompressOffsetReader(mParam.disableGuardOffset)
    , mUncompressOffsetReader(mParam.offsetThreshold, mParam.disableGuardOffset)
{
}

VarLenOffsetReader::~VarLenOffsetReader() {}

void VarLenOffsetReader::Init(uint32_t docCount, const FileReaderPtr& fileReader,
                              const SliceFileReaderPtr& extFileReader)
{
    if (mParam.equalCompressOffset) {
        mCompressOffsetReader.Init(docCount, fileReader, extFileReader);
    } else {
        mUncompressOffsetReader.Init(docCount, fileReader, extFileReader);
    }
}

bool VarLenOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    if (mParam.equalCompressOffset) {
        return mCompressOffsetReader.SetOffset(docId, offset);
    }
    return mUncompressOffsetReader.SetOffset(docId, offset);
}

const file_system::FileReaderPtr& VarLenOffsetReader::GetFileReader() const
{
    if (mParam.equalCompressOffset) {
        return mCompressOffsetReader.GetFileReader();
    }
    return mUncompressOffsetReader.GetFileReader();
}
}} // namespace indexlib::index
