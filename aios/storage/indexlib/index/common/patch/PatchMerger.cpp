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
#include "indexlib/index/common/patch/PatchMerger.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/SnappyCompressFileWriter.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PatchMerger);

Status PatchMerger::CopyFile(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                             const std::string& srcFileName,
                             const std::shared_ptr<indexlib::file_system::FileWriter>& dstFileWriter)
{
    const int COPY_BUFFER_SIZE = 2 * 1024 * 1024; // 2MB
    std::vector<uint8_t> copyBuffer;
    copyBuffer.resize(COPY_BUFFER_SIZE);
    char* buffer = (char*)copyBuffer.data();

    auto [status, srcFileReader] =
        directory->CreateFileReader(srcFileName, indexlib::file_system::FSOT_BUFFERED).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "creat file reader fail for file [%s].", srcFileName.c_str());
    size_t blockCount = (srcFileReader->GetLength() + COPY_BUFFER_SIZE - 1) / COPY_BUFFER_SIZE;
    for (size_t i = 0; i < blockCount; i++) {
        auto [status, readSize] = srcFileReader->Read(buffer, COPY_BUFFER_SIZE).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "read patch file [%s] fail.", srcFileName.c_str());
        assert(readSize > 0);
        status = dstFileWriter->Write(buffer, readSize).Status();
        RETURN_IF_STATUS_ERROR(status, "write  patch file fail.");
    }
    status = srcFileReader->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "close patch source file fail.");
    status = dstFileWriter->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "close patch dest file fail.");
    return Status::OK();
}

std::shared_ptr<indexlib::file_system::FileWriter>
PatchMerger::ConvertToCompressFileWriter(const std::shared_ptr<indexlib::file_system::FileWriter>& fileWriter,
                                         bool compressPatch)
{
    if (!compressPatch) {
        return fileWriter;
    }
    auto compressWriter = std::make_shared<indexlib::file_system::SnappyCompressFileWriter>();
    compressWriter->Init(fileWriter);
    return compressWriter;
}

} // namespace indexlibv2::index
