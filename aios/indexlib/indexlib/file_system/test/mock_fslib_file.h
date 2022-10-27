#ifndef __INDEXLIB_MOCK_FSLIB_FILE_H
#define __INDEXLIB_MOCK_FSLIB_FILE_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(file_system);

class MockFslibFile : public fslib::fs::File
{
public:
    MockFslibFile(const std::string& fileName, fslib::ErrorCode ec = fslib::EC_OK)
        : File(fileName, ec)
    { }
    ~MockFslibFile() {}

public:
    MOCK_METHOD2(read, ssize_t(void* buffer, size_t length));
    MOCK_METHOD2(write, ssize_t(const void* buffer, size_t length));
    MOCK_METHOD3(pread, ssize_t(void* buffer, size_t length, off_t offset));
    MOCK_METHOD3(pwrite, ssize_t(const void* buffer, size_t length, off_t offset));
    MOCK_METHOD0(flush, fslib::ErrorCode());
    MOCK_METHOD0(close, fslib::ErrorCode());
    MOCK_METHOD2(seek, fslib::ErrorCode(int64_t offset, fslib::SeekFlag flag));
    MOCK_METHOD0(tell, int64_t());
    MOCK_CONST_METHOD0(isOpened, bool());
    MOCK_METHOD0(isEof, bool());

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MockFslibFile);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MOCK_FSLIB_FILE_H
