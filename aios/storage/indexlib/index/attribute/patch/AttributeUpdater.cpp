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
#include "indexlib/index/attribute/patch/AttributeUpdater.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/SnappyCompressFileWriter.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeUpdater);

AttributeUpdater::AttributeUpdater(segmentid_t segId, const std::shared_ptr<config::IIndexConfig>& indexConfig)
    : _segmentId(segId)
{
    _attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
    assert(_attrConfig != nullptr);
}

std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
AttributeUpdater::CreatePatchFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                        const std::string& fileName)
{
    auto [status, patchFileWriter] =
        directory->CreateFileWriter(fileName, indexlib::file_system::WriterOption()).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "create patch file writer failed for [%s].", fileName.c_str());
    if (!_attrConfig->GetCompressType().HasPatchCompress()) {
        return std::make_pair(Status::OK(), patchFileWriter);
    }
    auto compressWriter = std::make_shared<indexlib::file_system::SnappyCompressFileWriter>();
    compressWriter->Init(patchFileWriter, DEFAULT_COMPRESS_BUFF_SIZE);
    return std::make_pair(Status::OK(), compressWriter);
}

} // namespace indexlibv2::index
