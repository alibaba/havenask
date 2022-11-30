#include "aitheta_indexer/plugins/aitheta/util/output_storage.h"

#include <indexlib/file_system/buffered_file_writer.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/storage/file_system_wrapper.h>

using namespace aitheta;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, OutputStorage);
IE_LOG_SETUP(aitheta_plugin, OutputStorage::Handler);
// for writer
IndexStorage::Handler::Pointer OutputStorage::create(const std::string &fileName, size_t size) {
    size_t nameLen = fileName.size();
    mWriter->Write(&nameLen, sizeof(nameLen));
    mWriter->Write(fileName.data(), nameLen * sizeof(char));
    mWriter->Write(&size, sizeof(size));
    return IndexStorage::Handler::Pointer(new OutputStorage::Handler(mWriter, size));
}

// for read
IndexStorage::Handler::Pointer OutputStorage::open(const std::string &fileName, bool readonly) { return nullptr; }

size_t OutputStorage::Handler::write(const void *data, size_t len) {
    // need not check return value
    return mWriter->Write(data, len);
}

size_t OutputStorage::Handler::write(size_t offset, const void *data, size_t len) {
    if (0U == offset) {
        return mWriter->Write(data, len);
    } else {
        IE_LOG(ERROR, "does not offer such interface with offset param");
        return 0;
    }
}

IE_NAMESPACE_END(aitheta_plugin);
