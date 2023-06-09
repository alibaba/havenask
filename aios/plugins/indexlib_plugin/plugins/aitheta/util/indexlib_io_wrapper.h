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
#ifndef __INDEXLIB_IO_WRAPPER_H
#define __INDEXLIB_IO_WRAPPER_H

#include <string>

#include "indexlib/util/Exception.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "indexlib/file_system/file/FileReader.h"

namespace indexlib {
namespace aitheta_plugin {

class IndexlibIoWrapper {
 public:
    template <typename T>
    static size_t Write(indexlib::file_system::FileWriterPtr &fw, const T *value, size_t n);
    template <typename T>
    static size_t Read(indexlib::file_system::FileReaderPtr &fr, T *value, size_t n);
    static indexlib::file_system::FileWriterPtr
        CreateWriter(const indexlib::file_system::DirectoryPtr &dir, const std::string &name);
    static indexlib::file_system::FileWriterPtr
        CreateWriter(const std::string &dirPath, const std::string &fileName);

    static indexlib::file_system::FileReaderPtr
        CreateReader(const indexlib::file_system::DirectoryPtr &dir, const std::string &fname,
                     indexlib::file_system::FSOpenType type = indexlib::file_system::FSOT_BUFFERED);
    static indexlib::file_system::FileReaderPtr
        CreateReader(const std::string &dirPath, const std::string &fname,
                     indexlib::file_system::FSOpenType type = indexlib::file_system::FSOT_BUFFERED);

    static indexlib::file_system::DirectoryPtr CreateDirectory(const std::string &dirPath);

 private:
    IE_LOG_DECLARE();
};

template <typename T>
size_t IndexlibIoWrapper::Write(indexlib::file_system::FileWriterPtr &fw, const T *value, size_t n) {
    auto [ec, writeLen] = fw->Write(static_cast<const void *>(value), n);
    if (ec != indexlib::file_system::FSEC_OK) {
        IE_LOG(ERROR, "fails to write data: path[%s], size[%u], ec[%d]", fw->DebugString().data(), n, ec);
        return 0U;
    }
    return writeLen;
}

template <typename T>
size_t IndexlibIoWrapper::Read(indexlib::file_system::FileReaderPtr &fr, T *value, size_t n) {
    auto [ec, readLen] = fr->Read(static_cast<void *>(value), n);
    if (ec != indexlib::file_system::FSEC_OK) {
        IE_LOG(ERROR, "fails to read data: path[%s], size[%u], ec[%d]", fr->DebugString().data(), n, ec);
        return 0U;
    }
    return readLen;
}
}
}

#endif  //  __INDEXLIB_IO_WRAPPER_H
