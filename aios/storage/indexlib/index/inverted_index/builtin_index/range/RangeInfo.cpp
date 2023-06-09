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
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/inverted_index/Common.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, RangeInfo);

RangeInfo::RangeInfo() : _minNumber(std::numeric_limits<uint64_t>::max()), _maxNumber(0) {}

RangeInfo::~RangeInfo() {}

void RangeInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    uint64_t minNumber = _minNumber;
    uint64_t maxNumber = _maxNumber;
    if (json.GetMode() == TO_JSON) {
        json.Jsonize("min_number", minNumber);
        json.Jsonize("max_number", maxNumber);
    } else {
        json.Jsonize("min_number", minNumber, minNumber);
        json.Jsonize("max_number", maxNumber, maxNumber);
        _minNumber = minNumber;
        _maxNumber = maxNumber;
    }
}

Status RangeInfo::Load(const std::shared_ptr<file_system::IDirectory>& directory)
{
    auto ret = directory->IsExist(RANGE_INFO_FILE_NAME);
    RETURN_IF_STATUS_ERROR(ret.Status(), "check file is exist fail, file[%s]", RANGE_INFO_FILE_NAME.c_str());
    if (!ret.result) {
        return Status::NotFound("%s file not found", RANGE_INFO_FILE_NAME.c_str());
    }

    auto retFileReader = directory->CreateFileReader(RANGE_INFO_FILE_NAME, file_system::FSOT_MEM);
    RETURN_IF_STATUS_ERROR(retFileReader.Status(), "create file reader fail, file[%s]", RANGE_INFO_FILE_NAME.c_str());
    auto fileReader = retFileReader.result;

    int64_t fileLen = fileReader->GetLogicLength();
    std::string content;
    content.resize(fileLen);
    char* data = const_cast<char*>(content.c_str());
    auto retRead = fileReader->Read(data, fileLen, 0);
    RETURN_IF_STATUS_ERROR(retRead.Status(), "read file fail, file[%s/%s] errorCode[%d]",
                           directory->GetLogicalPath().c_str(), RANGE_INFO_FILE_NAME.c_str(), (int)retRead.ec);
    autil::legacy::FromJsonString(*this, content);
    return Status::OK();
}

Status RangeInfo::Store(const std::shared_ptr<file_system::IDirectory>& directory)
{
    std::string content = autil::legacy::ToJsonString(*this);
    auto ret = directory->Store(RANGE_INFO_FILE_NAME, content, file_system::WriterOption());
    RETURN_IF_STATUS_ERROR(ret.Status(), "write file fail, file[%s/%s] errorCode[%d]",
                           directory->GetLogicalPath().c_str(), RANGE_INFO_FILE_NAME.c_str(), (int)ret.ec);
    return Status::OK();
}
} // namespace indexlib::index
