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
#include "indexlib_plugin/plugins/aitheta/util/input_storage.h"

using namespace aitheta;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, InputStorage);
IE_LOG_SETUP(aitheta_plugin, InputStorage::Handler);

bool InputStorage::access(const std::string &path) const {
    const int8_t *indexAddr = mOffset;
    size_t nameLen = 0U;
    memcpy(&nameLen, indexAddr, sizeof(nameLen));
    indexAddr += sizeof(nameLen);

    std::string name;
    name.assign((const char *)indexAddr, nameLen);
    if (path != name) {
        return false;
    }
    return false;
}

// !for read
IndexStorage::Handler::Pointer InputStorage::open(const std::string &fname, bool readonly) {
    const int8_t *indexAddr = mOffset;
    size_t nameLen = 0U;
    memcpy(&nameLen, indexAddr, sizeof(nameLen));
    indexAddr += sizeof(nameLen);

    std::string name(nameLen, 0);
    memcpy(const_cast<char *>(&name[0]), indexAddr, nameLen);
    indexAddr += nameLen;
    if (fname != name) {
        IE_LOG(ERROR, "file err: expected[%s], actual[%s]", fname.data(), name.data());
        return nullptr;
    }
    size_t size = 0U;
    memcpy(&size, indexAddr, sizeof(size));
    indexAddr += sizeof(size);

    mOffset = indexAddr;
    return IndexStorage::Handler::Pointer(new InputStorage::Handler(mOffset, size));
}

IndexStorage::Handler::Pointer InputStorage::create(const std::string &fname, size_t size) { return nullptr; }

size_t InputStorage::Handler::read(const void **data, size_t len) {
    if (mOffset + len > mIndexBaseAddr + mSize) {
        IE_LOG(ERROR,
               "out of range: mOffset[%p], len[%lu], mIndexBaseAddr[%p], "
               "mSize[%lu]",
               mOffset, len, mIndexBaseAddr, mSize);
        *data = 0;
        return 0;
    }
    *data = static_cast<const void *>(mOffset);
    mOffset += len;
    return len;
}

size_t InputStorage::Handler::read(size_t offset, const void **data, size_t len) {
    if (offset + len > mSize) {
        IE_LOG(ERROR,
               "out of range: mOffset[%p], len[%lu], offset[%lu] "
               "mIndexBaseAddr[%p], mSize[%lu]",
               mOffset, len, offset, mIndexBaseAddr, mSize);
        *data = 0;
        return 0;
    }

    *data = static_cast<const void *>(mIndexBaseAddr + offset);
    return len;
}

size_t InputStorage::Handler::read(void *data, size_t len) {
    memcpy(data, mOffset, len);
    mOffset += len;
    return len;
}

size_t InputStorage::Handler::read(size_t offset, void *data, size_t len) {
    memcpy(data, mIndexBaseAddr + offset, len);
    return len;
}

}
}
