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
#include "indexlib/index/common/data_structure/VarLenDataReader.h"

#include "indexlib/file_system/IDirectory.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, VarLenDataReader);

VarLenDataReader::VarLenDataReader(const VarLenDataParam& param, bool isOnline)
    : _offsetReader(param)
    , _offlineOffsetReader(param)
    , _dataBaseAddr(nullptr)
    , _dataLength(0)
    , _param(param)
    , _isOnline(isOnline)
    , _dataFileCompress(false)
{
}

VarLenDataReader::~VarLenDataReader() {}

Status VarLenDataReader::Init(uint32_t docCount, const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                              const std::string& offsetFileName, const std::string& dataFileName)
{
    std::shared_ptr<indexlib::file_system::FileReader> offsetFileReader;
    std::shared_ptr<indexlib::file_system::FileReader> dataFileReader;
    Status status;
    if (_isOnline) {
        indexlib::file_system::ReaderOption option(indexlib::file_system::FSOT_LOAD_CONFIG);
        option.supportCompress = true;
        std::tie(status, offsetFileReader) = directory->CreateFileReader(offsetFileName, option).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "create offset file reader fail");
        std::tie(status, dataFileReader) = directory->CreateFileReader(dataFileName, option).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "create data file reader fail");
    } else {
        indexlib::file_system::ReaderOption offsetOption(indexlib::file_system::FSOT_MEM);
        offsetOption.supportCompress = true;
        std::tie(status, offsetFileReader) = directory->CreateFileReader(offsetFileName, offsetOption).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "create offset file reader fail");
        indexlib::file_system::ReaderOption dataOption(indexlib::file_system::FSOT_BUFFERED);
        dataOption.supportCompress = true;
        std::tie(status, dataFileReader) = directory->CreateFileReader(dataFileName, dataOption).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "create data file reader fail");
    }
    status = _offsetReader.Init(docCount, offsetFileReader, nullptr);
    RETURN_IF_STATUS_ERROR(status, "init offset reader fail");
    if (!_isOnline) {
        _offlineOffsetReader = _offsetReader.CreateSessionReader(&_offlinePool);
    }
    _dataFileReader = dataFileReader;

    _dataFileCompress =
        std::dynamic_pointer_cast<indexlib::file_system::CompressFileReader>(_dataFileReader) != nullptr;
    _dataLength = _dataFileReader->GetLogicLength();
    _dataBaseAddr = (char*)_dataFileReader->GetBaseAddress();
    return Status::OK();
}

size_t VarLenDataReader::EvaluateCurrentMemUsed()
{
    size_t totalDataLength = 0;
    if (_dataFileReader) {
        totalDataLength += _dataFileReader->GetLogicLength();
    }
    auto offsetFileReader = _offsetReader.GetFileReader();
    if (offsetFileReader) {
        totalDataLength += offsetFileReader->GetLogicLength();
    }
    return totalDataLength;
}

} // namespace indexlibv2::index
