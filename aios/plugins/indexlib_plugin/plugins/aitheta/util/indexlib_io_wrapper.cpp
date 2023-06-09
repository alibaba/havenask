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
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"

#include <assert.h>

#include <map>
#include <string>

#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "indexlib/indexlib.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace indexlib::file_system;
using namespace std;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, IndexlibIoWrapper);

FileWriterPtr IndexlibIoWrapper::CreateWriter(const DirectoryPtr &dir, const string &name) {
    try {
        return dir->CreateFileWriter(name);
    } catch (const util::ExceptionBase &e) {
        IE_LOG(WARN, "failed to create file writer: dir[%s], file name[%s], msg[%s]", dir->DebugString().data(),
               name.data(), e.GetMessage().data());
        return FileWriterPtr();
    }
}

FileWriterPtr IndexlibIoWrapper::CreateWriter(const string &dirPath, const string &name) {
    indexlib::file_system::BufferedFileWriterPtr fw(new indexlib::file_system::BufferedFileWriter());
    auto absPath = FslibWrapper::JoinPath(dirPath, name);
    if (auto ec = fw->Open(absPath, absPath).Code(); ec != FSEC_OK) {
        IE_LOG(WARN, "failed to create file writer: dir[%s], file name[%s], ec[%d]", dirPath.data(), name.data(), ec);
        return nullptr;
    }
    return fw;
}

FileReaderPtr IndexlibIoWrapper::CreateReader(const DirectoryPtr &dir, const string &name, FSOpenType type) {
    try {
        return dir->CreateFileReader(name, type);
    } catch (const util::ExceptionBase &e) {
        IE_LOG(WARN, "failed to create file reader: dir[%s], file name[%s], msg[%s]", dir->DebugString().data(),
               name.data(), e.GetMessage().data());
        return FileReaderPtr();
    }
}

FileReaderPtr IndexlibIoWrapper::CreateReader(const string &dirPath, const string &name, FSOpenType type) {
    try {
        DirectoryPtr indexDir = CreateDirectory(dirPath);
        return indexDir->CreateFileReader(name, type);
    } catch (const util::ExceptionBase &e) {
        IE_LOG(WARN, "failed to create file reader: dir[%s], file name[%s], msg[%s]", dirPath.data(), name.data(),
               e.GetMessage().data());
        return FileReaderPtr();
    }
}

DirectoryPtr IndexlibIoWrapper::CreateDirectory(const std::string &dirPath) {
    try {
        indexlib::file_system::FileSystemOptions options;
        indexlib::util::MemoryQuotaControllerPtr controller(
            new indexlib::util::MemoryQuotaController(128l * 1024l * 1024l));
        if (nullptr == controller) {
            IE_LOG(ERROR, "fails to create controller");
        }
        options.memoryQuotaController = indexlib::util::PartitionMemoryQuotaControllerPtr(
            new indexlib::util::PartitionMemoryQuotaController(controller));
        auto fs = indexlib::file_system::FileSystemCreator::Create(/*name=*/"uninitialized-name",
                /*rootPath=*/dirPath, options, indexlib::util::MetricProviderPtr(), /*isOverride=*/false).GetOrThrow();
        return Directory::Get(fs);
    } catch (const util::ExceptionBase &e) {
        IE_LOG(WARN, "failed to create directory: dir[%s], msg[%s]", dirPath.data(), e.GetMessage().data());
        return DirectoryPtr();
    }
}

}
}
