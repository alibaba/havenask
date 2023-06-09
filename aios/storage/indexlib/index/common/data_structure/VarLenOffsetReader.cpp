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
#include "indexlib/index/common/data_structure/VarLenOffsetReader.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, VarLenOffsetReader);

VarLenOffsetReader::VarLenOffsetReader(const VarLenDataParam& param)
    : _param(param)
    , _compressOffsetReader(_param.disableGuardOffset)
    , _uncompressOffsetReader(_param.offsetThreshold, _param.disableGuardOffset)
{
}

VarLenOffsetReader::~VarLenOffsetReader() {}

Status VarLenOffsetReader::Init(uint32_t docCount, const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                                const std::shared_ptr<indexlib::file_system::SliceFileReader>& extFileReader)
{
    if (_param.equalCompressOffset) {
        return _compressOffsetReader.Init(docCount, fileReader, extFileReader);
    }
    return _uncompressOffsetReader.Init(docCount, fileReader, extFileReader);
}

std::pair<Status, bool> VarLenOffsetReader::SetOffset(docid_t docId, uint64_t offset)
{
    if (_param.equalCompressOffset) {
        return std::make_pair(Status::OK(), _compressOffsetReader.SetOffset(docId, offset));
    }
    return _uncompressOffsetReader.SetOffset(docId, offset);
}

const std::shared_ptr<indexlib::file_system::FileReader>& VarLenOffsetReader::GetFileReader() const
{
    if (_param.equalCompressOffset) {
        return _compressOffsetReader.GetFileReader();
    }
    return _uncompressOffsetReader.GetFileReader();
}

} // namespace indexlibv2::index
