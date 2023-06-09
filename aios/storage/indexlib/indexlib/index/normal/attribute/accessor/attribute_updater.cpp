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
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"

#include "indexlib/file_system/file/SnappyCompressFileWriter.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeUpdater);

std::shared_ptr<file_system::FileWriter>
AttributeUpdater::CreatePatchFileWriter(const file_system::DirectoryPtr& directory, const std::string& fileName)
{
    auto patchFileWriter = directory->CreateFileWriter(fileName);
    if (!mAttrConfig->GetCompressType().HasPatchCompress()) {
        return patchFileWriter;
    }
    file_system::SnappyCompressFileWriterPtr compressWriter(new file_system::SnappyCompressFileWriter);
    compressWriter->Init(patchFileWriter, DEFAULT_COMPRESS_BUFF_SIZE);
    return compressWriter;
}
}} // namespace indexlib::index
