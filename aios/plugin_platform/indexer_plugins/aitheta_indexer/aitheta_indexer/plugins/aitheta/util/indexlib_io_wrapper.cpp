#include "aitheta_indexer/plugins/aitheta/util/indexlib_io_wrapper.h"

#include <assert.h>

#include <map>
#include <string>

#include <indexlib/file_system/buffered_file_writer.h>
#include <indexlib/file_system/directory_creator.h>
#include <indexlib/file_system/file_reader.h>
#include <indexlib/file_system/indexlib_file_system_impl.h>
#include <indexlib/indexlib.h>
#include <indexlib/storage/file_system_wrapper.h>

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);

using namespace std;

IE_NAMESPACE_BEGIN(aitheta_plugin);
IE_LOG_SETUP(aitheta_plugin, IndexlibIoWrapper);

FileWriterPtr IndexlibIoWrapper::CreateWriter(const DirectoryPtr &dir, const string &name) {
    try {
        return dir->CreateFileWriter(name);
    } catch (const misc::ExceptionBase &e) {
        IE_LOG(ERROR, "failed to create file writer: dir[%s], file name[%s], msg[%s]", dir->GetPath().data(),
               name.data(), e.GetMessage().data());
        return FileWriterPtr();
    }
}

FileWriterPtr IndexlibIoWrapper::CreateWriter(const string &dirPath, const string &name) {
    try {
        IE_NAMESPACE(file_system)::BufferedFileWriterPtr fw(new IE_NAMESPACE(file_system)::BufferedFileWriter());
        auto absPath = FileSystemWrapper::JoinPath(dirPath, name);
        fw->Open(absPath);
        return fw;
    } catch (const misc::ExceptionBase &e) {
        IE_LOG(ERROR, "failed to create file writer: dir[%s], file name[%s], msg[%s]", dirPath.data(), name.data(),
               e.GetMessage().data());
        return FileWriterPtr();
    }
}

FileReaderPtr IndexlibIoWrapper::CreateReader(const DirectoryPtr &dir, const string &name, FSOpenType type) {
    try {
        return dir->CreateFileReader(name, type);
    } catch (const misc::ExceptionBase &e) {
        IE_LOG(ERROR, "failed to create file reader: dir[%s], file name[%s], msg[%s]", dir->GetPath().data(),
               name.data(), e.GetMessage().data());
        return FileReaderPtr();
    }
}

FileReaderPtr IndexlibIoWrapper::CreateReader(const string &dirPath, const string &name, FSOpenType type) {
    try {
        DirectoryPtr indexDir = CreateDirectory(dirPath);
        return indexDir->CreateFileReader(name, type);
    } catch (const misc::ExceptionBase &e) {
        IE_LOG(ERROR, "failed to create file reader: dir[%s], file name[%s], msg[%s]", dirPath.data(), name.data(),
               e.GetMessage().data());
        return FileReaderPtr();
    }
}

DirectoryPtr IndexlibIoWrapper::CreateDirectory(const std::string &dirPath) {
    try {
        IE_NAMESPACE(file_system)::IndexlibFileSystemPtr fs(new IndexlibFileSystemImpl(dirPath));
        IE_NAMESPACE(file_system)::FileSystemOptions options;
        IE_NAMESPACE(util)::MemoryQuotaControllerPtr controller(
            new IE_NAMESPACE(util)::MemoryQuotaController(128l * 1024l * 1024l));
        if (nullptr == controller) {
            IE_LOG(ERROR, "fails to create controller");
        }
        options.memoryQuotaController = IE_NAMESPACE(util)::PartitionMemoryQuotaControllerPtr(
            new IE_NAMESPACE(util)::PartitionMemoryQuotaController(controller));
        fs->Init(options);

        return DirectoryCreator::Get(fs, dirPath, true);
    } catch (const misc::ExceptionBase &e) {
        IE_LOG(ERROR, "failed to create directory: dir[%s], msg[%s]", dirPath.data(), e.GetMessage().data());
        return DirectoryPtr();
    }
}

IE_NAMESPACE_END(aitheta_plugin);
