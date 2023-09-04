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
#include "indexlib/base/Status.h"
#include "indexlib/index/deletionmap/Common.h"

namespace indexlib::util {
class Bitmap;
}

namespace indexlib::file_system {
class FileWriter;
}

namespace indexlibv2::index {
// TODO: rm
struct DeletionMapFileHeader {
    DeletionMapFileHeader(uint32_t itemCount = 0, uint32_t dataSize = 0)
        : bitmapItemCount(itemCount)
        , bitmapDataSize(dataSize)
    {
    }
    uint32_t bitmapItemCount;
    uint32_t bitmapDataSize;
};

class DeletionMapUtil
{
public:
    static Status DumpBitmap(const std::shared_ptr<indexlib::file_system::FileWriter>& writer,
                             indexlib::util::Bitmap* bitmap, uint32_t itemCount);

    static std::string GetDeletionMapFileName(segmentid_t segmentId);
    static std::pair<bool, segmentid_t> ExtractDeletionMapFileName(const std::string& fileName);

private:
    static Status DumpFileHeader(const std::shared_ptr<indexlib::file_system::FileWriter>& fileWriter,
                                 const DeletionMapFileHeader& fileHeader);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
