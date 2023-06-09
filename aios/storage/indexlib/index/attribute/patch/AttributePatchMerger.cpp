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
#include "indexlib/index/attribute/patch/AttributePatchMerger.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/attribute/Common.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributePatchMerger);

std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
AttributePatchMerger::CreateTargetPatchFileWriter(std::shared_ptr<indexlib::file_system::IDirectory> targetSegmentDir,
                                                  segmentid_t srcSegmentId, segmentid_t targetSegmentId)
{
    auto [status1, targetAttrDir] = targetSegmentDir->GetDirectory(ATTRIBUTE_INDEX_TYPE_STR).StatusWith();
    RETURN2_IF_STATUS_ERROR(status1, nullptr, "get attribute dir failed, error[%s]", status1.ToString().c_str());
    assert(targetAttrDir != nullptr);

    const std::string& fieldName = _attrConfig->GetAttrName();
    auto [status2, targetFieldDir] = targetAttrDir->GetDirectory(fieldName).StatusWith();
    RETURN2_IF_STATUS_ERROR(status2, nullptr, "get field dir failed, field:%s", fieldName.c_str());

    std::string patchFileName = autil::StringUtil::toString(srcSegmentId) + "_" +
                                autil::StringUtil::toString(targetSegmentId) + "." + PATCH_FILE_NAME;
    auto [status3, targetPatchFileWriter] =
        targetFieldDir->CreateFileWriter(patchFileName, indexlib::file_system::WriterOption()).StatusWith();
    RETURN2_IF_STATUS_ERROR(status3, nullptr, "create patch file writer failed for patch file: %s",
                            patchFileName.c_str());
    return {Status::OK(), targetPatchFileWriter};
}

} // namespace indexlibv2::index
