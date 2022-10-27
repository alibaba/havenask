#ifndef __INDEXLIB_ASYNCBUFFEREDFILEWRAPPERTEST_H
#define __INDEXLIB_ASYNCBUFFEREDFILEWRAPPERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/async_buffered_file_wrapper.h"
#include "indexlib/storage/test/buffered_file_wrapper_unittest.h"

IE_NAMESPACE_BEGIN(storage);

class AsyncBufferedFileWrapperTest : public BufferedFileWrapperTest
{
public:
    AsyncBufferedFileWrapperTest() { mAsync = true; }
    ~AsyncBufferedFileWrapperTest() {}

    DECLARE_CLASS_NAME(AsyncBufferedFileWrapperTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_ASYNCBUFFEREDFILEWRAPPERTEST_H
