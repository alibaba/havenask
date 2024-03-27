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
#include "indexlib/index/ann/aitheta2/util/segment_data/IndexDataAddrHolder.h"

#include "autil/legacy/exception.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
using namespace indexlib::file_system;
using namespace std;
namespace indexlibv2::index::ann {

bool IndexDataAddrHolder::Dump(const indexlib::file_system::DirectoryPtr& directory) const
{
    string content;
    try {
        content = autil::legacy::ToJsonString(*this);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "jsonize meta failed, error[%s]", e.what());
        return false;
    }

    try {
        auto removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        directory->RemoveFile(INDEX_ADDR_FILE, removeOption);
        auto writer = directory->CreateFileWriter(INDEX_ADDR_FILE);
        ANN_CHECK(writer != nullptr, "create failed");
        writer->Write(content.data(), content.size()).GetOrThrow();
        writer->Close().GetOrThrow();
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "dump failed, error[%s]", e.what());
        return false;
    }

    return true;
}

bool IndexDataAddrHolder::Load(const indexlib::file_system::DirectoryPtr& directory)
{
    indexlib::file_system::ReaderOption readerOption(FSOT_MMAP);
    readerOption.mayNonExist = true;

    try {
        auto reader = directory->CreateFileReader(INDEX_ADDR_FILE, readerOption);
        ANN_CHECK(reader != nullptr, "create file reader failed");
        string content((char*)reader->GetBaseAddress(), reader->GetLength());
        autil::legacy::FromJsonString(*this, content);
        reader->Close().GetOrThrow();
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "load failed, error[%s]", e.what());
        return false;
    }
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, IndexDataAddrHolder);
} // namespace indexlibv2::index::ann
