#include "aitheta_indexer/plugins/aitheta/util/input_storage.h"

using namespace aitheta;
IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, InputStorage);
IE_LOG_SETUP(aitheta_plugin, InputStorage::Handler);

bool InputStorage::access(const std::string &path) const {
    int8_t *indexAddr = mIndexAddr;
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
    int8_t *indexAddr = mIndexAddr;
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

    mIndexAddr = indexAddr;
    return IndexStorage::Handler::Pointer(new InputStorage::Handler(mIndexAddr, size));
}

IndexStorage::Handler::Pointer InputStorage::create(const std::string &fname, size_t size) { return nullptr; }

size_t InputStorage::Handler::read(const void **data, size_t len) {
    if (mIndexAddr + len > mIndexBaseAddr + mSize) {
        IE_LOG(ERROR,
               "out of range: mIndexAddr[%ld], len[%lu], mIndexBaseAddr[%ld], "
               "mSize[%lu]",
               (int64_t)mIndexAddr, len, (int64_t)mIndexBaseAddr, mSize);
        *data = 0;
        return 0;
    }
    *data = static_cast<void *>(mIndexAddr);
    mIndexAddr += len;
    return len;
}

size_t InputStorage::Handler::read(size_t offset, const void **data, size_t len) {
    if (offset + len > mSize) {
        IE_LOG(ERROR,
               "out of range: mIndexAddr[%ld], len[%lu], offset[%lu] "
               "mIndexBaseAddr[%ld], mSize[%lu]",
               (int64_t)mIndexAddr, len, offset, (int64_t)mIndexBaseAddr, mSize);
        *data = 0;
        return 0;
    }

    *data = static_cast<void *>(mIndexBaseAddr + offset);
    return len;
}

size_t InputStorage::Handler::read(void *data, size_t len) {
    memcpy(mIndexAddr, data, len);
    mIndexAddr += len;
    return len;
}

size_t InputStorage::Handler::read(size_t offset, void *data, size_t len) {
    memcpy(mIndexBaseAddr + offset, data, len);
    return len;
}

IE_NAMESPACE_END(aitheta_plugin);
