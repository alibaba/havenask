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
#include "indexlib/index/kv/DFSValueWriter.h"

#include "autil/Log.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/kv/SegmentStatistics.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, DFSValueWriter);

DFSValueWriter::DFSValueWriter(std::shared_ptr<indexlib::file_system::FileWriter> file)
    : _file(std::move(file))
    , _originalLength(0)
    , _compressedLength(0)
{
}

DFSValueWriter::~DFSValueWriter() {}

Status DFSValueWriter::Write(const autil::StringView& data)
{
    auto result = _file->Write(data.data(), data.size());
    return result.Status();
}

Status DFSValueWriter::Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    _originalLength = _file->GetLogicLength();
    _compressedLength = _file->GetLength();
    AUTIL_LOG(INFO, "value size: %ld bytes, after compress: %ld bytes", _originalLength, _compressedLength);
    return _file->Close().Status();
}

const char* DFSValueWriter::GetBaseAddress() const { return nullptr; }

int64_t DFSValueWriter::GetLength() const { return _file->GetLogicLength(); }

void DFSValueWriter::FillStatistics(SegmentStatistics& stat) const
{
    stat.valueMemoryUse = _originalLength;
    if (_originalLength != _compressedLength) {
        stat.valueCompressRatio = 1.0f * _compressedLength / _originalLength;
    }
}

void DFSValueWriter::UpdateMemoryUsage(MemoryUsage& memUsage) const {}

} // namespace indexlibv2::index
