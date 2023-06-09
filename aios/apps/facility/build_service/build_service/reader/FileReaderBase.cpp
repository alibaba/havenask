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
#include "build_service/reader/FileReaderBase.h"

#include "autil/StringUtil.h"
#include "build_service/reader/FileReader.h"
#include "build_service/reader/GZipFileReader.h"
#include "fslib/util/FileUtil.h"

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, FileReaderBase);

using namespace std;
using namespace autil;
using namespace build_service::util;

FileReaderBase* FileReaderBase::createFileReader(const string& fileName, uint32_t bufferSize)
{
    FileReaderBase* fReader = NULL;
    FileType fileType = getFileType(fileName);
    if (fileType == GZIP_TYPE) {
        fReader = new (nothrow) GZipFileReader(bufferSize);
    } else {
        fReader = new (nothrow) FileReader;
    }

    return fReader;
}

FileReaderBase::FileType FileReaderBase::getFileType(const string& fileName)
{
    string folder, file;
    fslib::util::FileUtil::splitFileName(fileName, folder, file);

    vector<string> exts = StringUtil::split(file, ".");
    if (exts.size() && (*exts.rbegin() == "gz")) {
        // this is a gzip file
        return GZIP_TYPE;
    } else {
        return FILE_TYPE;
    }
}

}} // namespace build_service::reader
