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
#include "indexlib/index/deletionmap/DeletionMapUtil.h"

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/util/Bitmap.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapUtil);

Status DeletionMapUtil::DumpFileHeader(const std::shared_ptr<indexlib::file_system::FileWriter>& fileWriter,
                                       const DeletionMapFileHeader& fileHeader)
{
    assert(fileWriter);
    auto ret = fileWriter->Write((void*)(&fileHeader.bitmapItemCount), sizeof(uint32_t));
    if (!ret.OK()) {
        AUTIL_LOG(ERROR, "write file [%s] failed", fileWriter->GetLogicalPath().c_str());
        return Status::IOError("File writer write fail");
    }
    ret = fileWriter->Write((void*)(&fileHeader.bitmapDataSize), sizeof(uint32_t));
    if (!ret.OK()) {
        AUTIL_LOG(ERROR, "write file [%s] failed", fileWriter->GetLogicalPath().c_str());
        return Status::IOError("File writer write fail");
    }
    return Status::OK();
}

Status DeletionMapUtil::DumpBitmap(const std::shared_ptr<indexlib::file_system::FileWriter>& writer,
                                   indexlib::util::Bitmap* bitmap, uint32_t itemCount)
{
    assert(bitmap);
    if (!writer || !bitmap) {
        AUTIL_LOG(ERROR, "file writer [%p] or bitmap [%p] is nullptr, dump bitmap failed", writer.get(), bitmap);
        return Status::InvalidArgs();
    }
    AUTIL_LOG(INFO, "begin dump bitmap to path [%s], item count [%u], set count [%u]", writer->DebugString().c_str(),
              itemCount, bitmap->GetSetCount());
    size_t dumpSize = indexlib::util::Bitmap::GetDumpSize(itemCount);
    size_t reserveSize = sizeof(DeletionMapFileHeader) + dumpSize;
    auto ret = writer->ReserveFile(reserveSize);
    if (!ret.OK()) {
        AUTIL_LOG(ERROR, "File writer reserve file fail, reserveSize[%lu] file[%s]", reserveSize,
                  writer->GetLogicalPath().c_str());
        return Status::IOError("reserve file failed");
    }

    DeletionMapFileHeader fileHeader(itemCount, dumpSize);
    RETURN_IF_STATUS_ERROR(DumpFileHeader(writer, fileHeader), "dump file header fail");

    uint32_t* data = bitmap->GetData();
    if (!writer->Write((void*)data, dumpSize).OK()) {
        AUTIL_LOG(ERROR, "Write file fail, size[%lu] file[%s]", dumpSize, writer->GetLogicalPath().c_str());
        return Status::IOError("Write file fail");
    }
    assert(reserveSize == writer->GetLength());
    return Status::OK();
}

std::string DeletionMapUtil::GetDeletionMapFileName(segmentid_t segmentId)
{
    std::stringstream ss;
    ss << "data_" << segmentId;
    return ss.str();
}

// TODO: rm
std::pair<bool, segmentid_t> DeletionMapUtil::ExtractDeletionMapFileName(const std::string& fileName)
{
    const std::string PREFIX = "data_";
    if (!autil::StringUtil::startsWith(fileName, PREFIX)) {
        return {false, INVALID_SEGMENTID};
    }
    segmentid_t targetSegment = INVALID_SEGMENTID;
    if (autil::StringUtil::numberFromString(fileName.substr(PREFIX.size()), targetSegment)) {
        return {true, targetSegment};
    }
    return {false, INVALID_SEGMENTID};
}

} // namespace indexlibv2::index
