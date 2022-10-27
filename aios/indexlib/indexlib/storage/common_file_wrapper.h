#ifndef __INDEXLIB_NORMALFILEWRAPPER_H
#define __INDEXLIB_NORMALFILEWRAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/storage/file_wrapper.h"

IE_NAMESPACE_BEGIN(storage);

// deprecated
class CommonFileWrapper : public FileWrapper
{    
public:
    CommonFileWrapper(fslib::fs::File* file, bool useDirectIO = false, bool needClose = true);
    ~CommonFileWrapper();
public:
    size_t Read(void* buffer, size_t length) override;
    size_t PRead(void* buffer, size_t length, off_t offset) override;
    future_lite::Future<size_t> PReadAsync(void* buffer, size_t length, off_t offset, int advice) override;
    size_t PReadV(const iovec* iov, int iovcnt, off_t offset) override;
    future_lite::Future<size_t>
    PReadVAsync(const iovec* iov, int iovcnt, off_t offset, int advice) override;
    void Write(const void* buffer, size_t length) override;

    // TODO: just override for common_file_wrapper_mock
    // mockstorage should be replaced by gmock
    void Close() override;

public:
    static size_t Read(fslib::fs::File *file, void *buffer, size_t length,
                       bool useDirectIO = false);
    static void Write(fslib::fs::File *file, const void *buffer, size_t length);

    future_lite::Future<size_t> InternalPReadASync(
        void* buffer, size_t length, off_t offset, int advice);

    future_lite::Future<size_t> SinglePreadAsync(
        void* buffer, size_t length, off_t offset, int advice);

private:
    static const size_t MIN_ALIGNMENT = 512;
    static constexpr size_t PANGU_MAX_READ_BYTES = 2 * 1024 * 1024;
    
    bool mUseDirectIO;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CommonFileWrapper);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_NORMALFILEWRAPPER_H
