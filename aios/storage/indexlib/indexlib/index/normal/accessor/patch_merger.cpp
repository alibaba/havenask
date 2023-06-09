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
#include "indexlib/index/normal/accessor/patch_merger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/SnappyCompressFileWriter.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PatchMerger);

PatchMerger::PatchMerger() {}

PatchMerger::~PatchMerger() {}

void PatchMerger::CopyFile(const file_system::DirectoryPtr& directory, const std::string& srcFileName,
                           const file_system::FileWriterPtr& dstFileWriter)
{
    const int COPY_BUFFER_SIZE = 2 * 1024 * 1024; // 2MB
    std::vector<uint8_t> copyBuffer;
    copyBuffer.resize(COPY_BUFFER_SIZE);
    char* buffer = (char*)copyBuffer.data();

    file_system::FileReaderPtr srcFileReader = directory->CreateFileReader(srcFileName, file_system::FSOT_BUFFERED);
    size_t blockCount = (srcFileReader->GetLength() + COPY_BUFFER_SIZE - 1) / COPY_BUFFER_SIZE;
    for (size_t i = 0; i < blockCount; i++) {
        size_t readSize = srcFileReader->Read(buffer, COPY_BUFFER_SIZE).GetOrThrow();
        assert(readSize > 0);
        dstFileWriter->Write(buffer, readSize).GetOrThrow();
    }
    srcFileReader->Close().GetOrThrow();
    dstFileWriter->Close().GetOrThrow();
}

file_system::FileWriterPtr PatchMerger::CreatePatchFileWriter(const file_system::FileWriterPtr& destPatchFile,
                                                              bool compressPatch)
{
    if (!compressPatch) {
        return destPatchFile;
    }
    file_system::SnappyCompressFileWriterPtr compressWriter(new file_system::SnappyCompressFileWriter);
    compressWriter->Init(destPatchFile);
    return compressWriter;
}
}} // namespace indexlib::index
