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
#include "indexlib/table/kv_table/index_task/KVTableBulkloadSegmentCreatePlan.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableBulkloadSegmentCreatePlan);

KVTableBulkloadSegmentCreatePlan::KVTableBulkloadSegmentCreatePlan(std::string name,
                                                                   framework::IndexTaskResourceType type)
    : IndexTaskResource(std::move(name), type)
{
}

Status
KVTableBulkloadSegmentCreatePlan::Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    auto status = resourceDirectory->GetIDirectory()->RemoveFile(BULKLOAD_SEGMENT_CREATE_PLAN, removeOption).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "remove file [%s] in directory [%s] failed, status:%s", BULKLOAD_SEGMENT_CREATE_PLAN,
                  resourceDirectory->DebugString().c_str(), status.ToString().c_str());
        return status;
    }

    std::string content = ToJsonString(*this);
    indexlib::file_system::WriterOption writerOption = indexlib::file_system::WriterOption::AtomicDump();
    writerOption.notInPackage = true;

    status = resourceDirectory->GetIDirectory()->Store(BULKLOAD_SEGMENT_CREATE_PLAN, content, writerOption).Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "store file [%s] in directory [%s] failed, status:%s", BULKLOAD_SEGMENT_CREATE_PLAN,
                  resourceDirectory->DebugString().c_str(), status.ToString().c_str());
    }
    return status;
}

Status
KVTableBulkloadSegmentCreatePlan::Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    std::string content;
    auto status =
        resourceDirectory->GetIDirectory()
            ->Load(BULKLOAD_SEGMENT_CREATE_PLAN,
                   indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM), content)
            .Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "load [%s] in dir [%s] failed, status:%s", BULKLOAD_SEGMENT_CREATE_PLAN,
                  resourceDirectory->DebugString().c_str(), status.ToString().c_str());
        return status;
    }
    try {
        autil::legacy::FromJsonString(*this, content);
    } catch (const autil::legacy::ExceptionBase& e) {
        status = Status::Corruption("invalid json str [%s] exception [%s]", content.c_str(), e.GetMessage().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
    } catch (const std::exception& e) {
        status = Status::Corruption("invalid json str [%s] exception [%s]", content.c_str(), e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
    } catch (...) {
        status = Status::Corruption("invalid json str [%s], unknown exception", content.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
    }
    return status;
}

} // namespace indexlibv2::table
