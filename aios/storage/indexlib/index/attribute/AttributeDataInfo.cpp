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
#include "indexlib/index/attribute/AttributeDataInfo.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlibv2.index, AttributeDataInfo);

Status AttributeDataInfo::Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, bool& isExist)
{
    Status status;
    std::tie(status, isExist) = directory->IsExist(ATTRIBUTE_DATA_INFO_FILE_NAME_).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "load attibute data from [%s] failed", directory->GetPhysicalPath("").c_str());
    if (!isExist) {
        return Status::OK();
    }
    std::string dataInfo;
    status = directory
                 ->Load(ATTRIBUTE_DATA_INFO_FILE_NAME_,
                        indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM), dataInfo)
                 .Status();
    RETURN_IF_STATUS_ERROR(status, "load attibute data from [%s] failed", directory->GetPhysicalPath("").c_str());
    FromString(dataInfo);
    return Status::OK();
}

Status AttributeDataInfo::Store(const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    auto status =
        directory->RemoveFile(ATTRIBUTE_DATA_INFO_FILE_NAME_, indexlib::file_system::RemoveOption::MayNonExist())
            .Status();
    if (!status.IsOK()) {
        return status;
    }
    std::string str = ToString();
    return directory->Store(ATTRIBUTE_DATA_INFO_FILE_NAME_, str, indexlib::file_system::WriterOption::AtomicDump())
        .Status();
}

void AttributeDataInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("uniq_item_count", uniqItemCount);
    json.Jsonize("max_item_length", maxItemLen);
    json.Jsonize("slice_count", sliceCount, sliceCount);
}

} // namespace indexlibv2::index
