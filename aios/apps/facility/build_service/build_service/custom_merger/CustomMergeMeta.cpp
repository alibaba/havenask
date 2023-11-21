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
#include "build_service/custom_merger/CustomMergeMeta.h"

#include <cstddef>

#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/fslib/DeleteOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"

using namespace std;

using namespace indexlib::file_system;

namespace build_service { namespace custom_merger {
BS_LOG_SETUP(custom_merger, CustomMergeMeta);

CustomMergeMeta::CustomMergeMeta() {}

CustomMergeMeta::~CustomMergeMeta() {}

void CustomMergeMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("custom_merge_plans", _mergePlans, _mergePlans);
}

void CustomMergeMeta::store(const std::string& path, FenceContext* fenceContext)
{
    if (FslibWrapper::IsExist(path).GetOrThrow()) {
        FslibWrapper::DeleteDirE(path, DeleteOption::Fence(fenceContext, false));
    }

    FslibWrapper::MkDirE(path, true);
    string content = ToJsonString(*this);
    string mergeMetaFile = FslibWrapper::JoinPath(path, "merge_meta");
    indexlib::file_system::ErrorCode ec = FslibWrapper::AtomicStore(mergeMetaFile, content, false, fenceContext).Code();
    if (ec == FSEC_EXIST) {
        AUTIL_LEGACY_THROW(indexlib::util::ExistException, "file [" + mergeMetaFile + "] already exist.");
    } else if (ec != FSEC_OK) {
        AUTIL_LEGACY_THROW(indexlib::util::FileIOException, "store file [" + mergeMetaFile + "] FAILED.");
    }
}

void CustomMergeMeta::load(const std::string& path)
{
    string mergeMetaFile = FslibWrapper::JoinPath(path, "merge_meta");
    string content;
    indexlib::file_system::ErrorCode ec = FslibWrapper::AtomicLoad(mergeMetaFile, content).Code();
    if (ec == FSEC_NOENT) {
        AUTIL_LEGACY_THROW(indexlib::util::NonExistException, "file [" + mergeMetaFile + "] not exist.");
    } else if (ec != FSEC_OK) {
        AUTIL_LEGACY_THROW(indexlib::util::FileIOException, "load file [" + mergeMetaFile + "] FAILED.");
    }
    FromJsonString(*this, content);
}

bool CustomMergeMeta::getMergePlan(size_t instanceId, CustomMergePlan& plan) const
{
    for (auto& mergePlan : _mergePlans) {
        if ((size_t)mergePlan.getInstanceId() == instanceId) {
            plan = mergePlan;
            return true;
        }
    }
    return false;
}

}} // namespace build_service::custom_merger
