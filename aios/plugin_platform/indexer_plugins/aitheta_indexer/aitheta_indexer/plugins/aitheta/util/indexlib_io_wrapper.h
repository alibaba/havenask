#ifndef __INDEXLIB_IO_WRAPPER_H
#define __INDEXLIB_IO_WRAPPER_H

#include <string>

#include <indexlib/misc/exception.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/buffered_file_writer.h>
#include <indexlib/file_system/directory.h>
#include <indexlib/indexlib.h>
#include <indexlib/file_system/file_reader.h>

IE_NAMESPACE_BEGIN(aitheta_plugin);

class IndexlibIoWrapper {
 public:
    template <typename T>
    static size_t Write(IE_NAMESPACE(file_system)::FileWriterPtr &fw, const T *value, size_t n);
    template <typename T>
    static size_t Read(IE_NAMESPACE(file_system)::FileReaderPtr &fr, T *value, size_t n);
    static IE_NAMESPACE(file_system)::FileWriterPtr
        CreateWriter(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, const std::string &name);
    static IE_NAMESPACE(file_system)::FileWriterPtr
        CreateWriter(const std::string &dirPath, const std::string &fileName);

    static IE_NAMESPACE(file_system)::FileReaderPtr
        CreateReader(const IE_NAMESPACE(file_system)::DirectoryPtr &dir, const std::string &fname,
                     IE_NAMESPACE(file_system)::FSOpenType type = IE_NAMESPACE(file_system)::FSOT_BUFFERED);
    static IE_NAMESPACE(file_system)::FileReaderPtr
        CreateReader(const std::string &dirPath, const std::string &fname,
                     IE_NAMESPACE(file_system)::FSOpenType type = IE_NAMESPACE(file_system)::FSOT_BUFFERED);

    static IE_NAMESPACE(file_system)::DirectoryPtr CreateDirectory(const std::string &dirPath);

 private:
    IE_LOG_DECLARE();
};

template <typename T>
size_t IndexlibIoWrapper::Write(IE_NAMESPACE(file_system)::FileWriterPtr &fw, const T *value, size_t n) {
    try {
        return fw->Write(static_cast<const void *>(value), n);
    } catch (const misc::ExceptionBase &e) {
        IE_LOG(ERROR, "fails to write data: path[%s], size[%u], msg[%s]", fw->GetPath().data(), n,
               e.GetMessage().data());
        return 0U;
    }
}

template <typename T>
size_t IndexlibIoWrapper::Read(IE_NAMESPACE(file_system)::FileReaderPtr &fr, T *value, size_t n) {
    try {
        return fr->Read(static_cast<void *>(value), n);
    } catch (const misc::ExceptionBase &e) {
        IE_LOG(ERROR, "fails to read data: path[%s], size[%u], msg[%s]", fr->GetPath().data(), n,
               e.GetMessage().data());
        return 0U;
    }
}
IE_NAMESPACE_END(aitheta_plugin);

#endif  //  __INDEXLIB_IO_WRAPPER_H
