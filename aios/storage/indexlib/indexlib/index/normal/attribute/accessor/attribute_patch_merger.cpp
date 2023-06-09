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
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"

#include "indexlib/config/attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/SnappyCompressFileWriter.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributePatchMerger);

AttributePatchMerger::AttributePatchMerger(const AttributeConfigPtr& attrConfig,
                                           const SegmentUpdateBitmapPtr& segUpdateBitmap)
    : mAttrConfig(attrConfig)
    , mSegUpdateBitmap(segUpdateBitmap)
{
}

AttributePatchMerger::~AttributePatchMerger() {}

// file_system::FileWriterPtr AttributePatchMerger::CreatePatchFileWriter(
//         const FileWriterPtr& destPatchFile) const
// {
//     if (!mAttrConfig->GetCompressType().HasPatchCompress())
//     {
//         return destPatchFile;
//     }
//     SnappyCompressFileWriterPtr compressWriter(new SnappyCompressFileWriter);
//     compressWriter->Init(destPatchFile);
//     return compressWriter;
// }
}} // namespace indexlib::index
