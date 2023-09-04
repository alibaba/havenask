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
#include "indexlib/table/index_task/VersionResource.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, VersionResource);

VersionResource::VersionResource(std::string name, framework::IndexTaskResourceType type)
    : IndexTaskResource(std::move(name), type)
{
}

VersionResource::~VersionResource() {}

Status VersionResource::Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    auto status = resourceDirectory->GetIDirectory()->RemoveFile(PARAM_TARGET_VERSION, removeOption).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "remove file [%s] in directory [%s] failed", PARAM_TARGET_VERSION,
                  resourceDirectory->DebugString().c_str());
        return status;
    }

    std::string content = ToJsonString(*this);
    indexlib::file_system::WriterOption writerOption = indexlib::file_system::WriterOption::AtomicDump();
    writerOption.notInPackage = true;

    status = resourceDirectory->GetIDirectory()->Store(PARAM_TARGET_VERSION, content, writerOption).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "store file [%s] in directory [%s] failed", PARAM_TARGET_VERSION,
                  resourceDirectory->DebugString().c_str());
        return status;
    }

    return Status::OK();
}
Status VersionResource::Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    std::string content;
    auto status =
        resourceDirectory->GetIDirectory()
            ->Load(PARAM_TARGET_VERSION,
                   indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM), content)
            .Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "segment info not exist in dir [%s]", resourceDirectory->DebugString().c_str());
        return status;
    }

    try {
        autil::legacy::FromJsonString(*this, content);
    } catch (const autil::legacy::ParameterInvalidException& e) {
        auto status =
            Status::ConfigError("Invalid json str [%s] exception [%s]", content.c_str(), e.GetMessage().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    return Status::OK();
}

void VersionResource::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { json.Jsonize("version", _version); }

} // namespace indexlibv2::table
