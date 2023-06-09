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
#include "indexlib/index/data_structure/var_len_data_reader.h"

#include "indexlib/file_system/file/CompressFileReader.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarLenDataReader);

VarLenDataReader::VarLenDataReader(const VarLenDataParam& param, bool isOnline)
    : mOffsetReader(param)
    , mOfflineOffsetReader(param)
    , mDataBaseAddr(nullptr)
    , mDataLength(0)
    , mParam(param)
    , mIsOnline(isOnline)
    , mDataFileCompress(false)
{
}

VarLenDataReader::~VarLenDataReader() {}

void VarLenDataReader::Init(uint32_t docCount, const DirectoryPtr& directory, const string& offsetFileName,
                            const string& dataFileName)
{
    FileReaderPtr offsetFileReader;
    FileReaderPtr dataFileReader;
    if (mIsOnline) {
        file_system::ReaderOption option(file_system::FSOT_LOAD_CONFIG);
        option.supportCompress = !mParam.dataCompressorName.empty();
        offsetFileReader = directory->CreateFileReader(offsetFileName, option);
        dataFileReader = directory->CreateFileReader(dataFileName, option);
    } else {
        file_system::ReaderOption offsetOption(file_system::FSOT_MEM);
        offsetOption.supportCompress = !mParam.dataCompressorName.empty();
        offsetFileReader = directory->CreateFileReader(offsetFileName, offsetOption);

        file_system::ReaderOption dataOption(file_system::FSOT_BUFFERED);
        dataOption.supportCompress = !mParam.dataCompressorName.empty();
        dataFileReader = directory->CreateFileReader(dataFileName, dataOption);
    }
    mOffsetReader.Init(docCount, offsetFileReader);
    if (!mIsOnline) {
        mOfflineOffsetReader = mOffsetReader.CreateSessionReader(&mOfflinePool);
    }
    mDataFileReader = dataFileReader;

    mDataFileCompress = DYNAMIC_POINTER_CAST(file_system::CompressFileReader, mDataFileReader) != nullptr;
    mDataLength = mDataFileReader->GetLogicLength();
    mDataBaseAddr = (char*)mDataFileReader->GetBaseAddress();
}
}} // namespace indexlib::index
