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
#pragma once

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"
#include "indexlib/table/kv_table/index_task/KVTableExternalFileImportJob.h"

namespace indexlibv2::table {

class KVTableBulkloadSegmentCreatePlan : public framework::IndexTaskResource, public autil::legacy::Jsonizable
{
public:
    KVTableBulkloadSegmentCreatePlan(std::string name, framework::IndexTaskResourceType type);
    ~KVTableBulkloadSegmentCreatePlan() = default;

    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;
    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("internal_file_infos", _internalFileInfos, _internalFileInfos);
    }

    const std::vector<InternalFileInfo>& GetInternalFileInfos() const { return _internalFileInfos; }
    void AddInternalFileInfo(const InternalFileInfo& internalFileInfo)
    {
        _internalFileInfos.emplace_back(internalFileInfo);
    }

private:
    std::vector<InternalFileInfo> _internalFileInfos;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
