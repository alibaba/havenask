#pragma once

#include <memory>

#include "fslib/fslib.h"

namespace indexlib { namespace file_system {

class MockFslibFile : public fslib::fs::File
{
public:
    MockFslibFile(const std::string& fileName, fslib::ErrorCode ec = fslib::EC_OK) : File(fileName, ec) {}
    ~MockFslibFile() {}

public:
    MOCK_METHOD(ssize_t, read, (void* buffer, size_t length), (override));
    MOCK_METHOD(ssize_t, write, (const void* buffer, size_t length), (override));
    MOCK_METHOD(ssize_t, pread, (void* buffer, size_t length, off_t offset), (override));
    MOCK_METHOD(ssize_t, pwrite, (const void* buffer, size_t length, off_t offset), (override));
    MOCK_METHOD(fslib::ErrorCode, flush, (), (override));
    MOCK_METHOD(fslib::ErrorCode, close, (), (override));
    MOCK_METHOD(fslib::ErrorCode, seek, (int64_t offset, fslib::SeekFlag flag), (override));
    MOCK_METHOD(int64_t, tell, (), (override));
    MOCK_METHOD(bool, isOpened, (), (const, override));
    MOCK_METHOD(bool, isEof, (), (override));

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MockFslibFile> MockFslibFilePtr;
}} // namespace indexlib::file_system
