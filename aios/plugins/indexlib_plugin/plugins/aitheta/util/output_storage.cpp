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
#include "indexlib_plugin/plugins/aitheta/util/output_storage.h"

#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace aitheta;

using namespace indexlib::file_system;
using namespace indexlib::file_system;

namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, OutputStorage);
IE_LOG_SETUP(aitheta_plugin, OutputStorage::Handler);
// for writer
IndexStorage::Handler::Pointer OutputStorage::create(const std::string &fileName, size_t size) {
    size_t nameLen = fileName.size();
    mWriter->Write(&nameLen, sizeof(nameLen)).GetOrThrow();;
    mWriter->Write(fileName.data(), nameLen * sizeof(char)).GetOrThrow();;
    mWriter->Write(&size, sizeof(size)).GetOrThrow();;
    return IndexStorage::Handler::Pointer(new OutputStorage::Handler(mWriter, size));
}

// for read
IndexStorage::Handler::Pointer OutputStorage::open(const std::string &fileName, bool readonly) { return nullptr; }

size_t OutputStorage::Handler::write(const void *data, size_t len) {
    // need not check return value
    return mWriter->Write(data, len).GetOrThrow();;
}

size_t OutputStorage::Handler::write(size_t offset, const void *data, size_t len) {
    if (0U == offset) {
        return mWriter->Write(data, len).GetOrThrow();;
    } else {
        IE_LOG(ERROR, "does not offer such interface with offset param");
        return 0;
    }
}

}
}
