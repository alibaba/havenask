#pragma once

#include <stdint.h>
#include <sys/types.h>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "swift/common/Common.h"

namespace swift {
namespace storage {

class FakeFslibFile : public fslib::fs::File {
public:
    FakeFslibFile(fslib::fs::File *file);
    ~FakeFslibFile();

private:
    FakeFslibFile(const FakeFslibFile &);
    FakeFslibFile &operator=(const FakeFslibFile &);

public:
    ssize_t read(void *buffer, size_t length) override { return _file->read(buffer, length); }

    ssize_t write(const void *buffer, size_t length) override { return _file->write(buffer, length); }

    ssize_t pread(void *buffer, size_t length, off_t offset) override { return _file->pread(buffer, length, offset); }

    ssize_t pwrite(const void *buffer, size_t length, off_t offset) override {
        return _file->pwrite(buffer, length, offset);
    }

    fslib::ErrorCode flush() override { return _file->flush(); }

    fslib::ErrorCode close() override { return _file->close(); }

    fslib::ErrorCode seek(int64_t offset, fslib::SeekFlag flag) override {
        if (-1 == _seekPosLimit) {
            return _file->seek(offset, flag);
        } else {
            if (offset <= _seekPosLimit) {
                return _file->seek(offset, flag);
            }
            return _file->seek(_seekPosLimit, flag);
        }
    }

    int64_t tell() override { return _file->tell(); }

    bool isOpened() const override { return _file->isOpened(); }

    bool isEof() override { return _file->isEof(); }

public:
    void setSeekPosLimit(int64_t seekPosLimit) { _seekPosLimit = seekPosLimit; }

private:
    fslib::fs::File *_file;
    int64_t _seekPosLimit;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeFslibFile);

} // namespace storage
} // namespace swift
