#ifndef __INDEXLIB_BUFFEREDFILEOUTPUTSTREAMTEST_H
#define __INDEXLIB_BUFFEREDFILEOUTPUTSTREAMTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/buffered_file_output_stream.h"

IE_NAMESPACE_BEGIN(file_system);

struct MockBufferedFileNode : public BufferedFileNode
{
    MockBufferedFileNode() {}

    MOCK_METHOD2(DoOpen, void(const std::string&, FSOpenType));
    MOCK_METHOD0(Close, void());

    size_t Write(const void* data, size_t length) override
    {
        dataBuffer += std::string((const char*)data, length);
        mLength += length;
        return length;
    }

    std::string dataBuffer;
};
DEFINE_SHARED_PTR(MockBufferedFileNode);




class BufferedFileOutputStreamTest : public INDEXLIB_TESTBASE
{
public:
    BufferedFileOutputStreamTest();
    ~BufferedFileOutputStreamTest();

    DECLARE_CLASS_NAME(BufferedFileOutputStreamTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BufferedFileOutputStreamTest, TestSimpleProcess);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BUFFEREDFILEOUTPUTSTREAMTEST_H
