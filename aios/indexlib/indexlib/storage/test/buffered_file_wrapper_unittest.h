#ifndef __INDEXLIB_BUFFEREDFILEWRAPPERTEST_H
#define __INDEXLIB_BUFFEREDFILEWRAPPERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/buffered_file_wrapper.h"

IE_NAMESPACE_BEGIN(storage);

class MockBufferedFileWrapper : public BufferedFileWrapper
{
public:
    MOCK_METHOD1(LoadBuffer, void(int64_t));

    MockBufferedFileWrapper(fslib::fs::File *file, fslib::Flag mode, 
                            int64_t fileLength, uint32_t bufferSize)
        : BufferedFileWrapper(file, mode, fileLength, bufferSize)
    {
        char *baseAddr = mBuffer->GetBaseAddr();
        for (uint32_t i = 0; i < bufferSize; i++)
        {
            baseAddr[i] = i % 128;
        }
    }
};

class BufferedFileWrapperTest : public INDEXLIB_TESTBASE
{
public:
    BufferedFileWrapperTest() { mAsync = false; }
    virtual ~BufferedFileWrapperTest() {}

    DECLARE_CLASS_NAME(BufferedFileWrapperTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForRead();
    void TestCaseForSeek();
    void TestCaseForReadBigFile();

    void TestCaseForWrite();    
    void TestCaseForWriteMultiTimes();

private:
    void TestForRead(uint32_t bufferSize, uint32_t dataLength);
    void TestForWrite(uint32_t lengthToWrite, uint32_t bufferSize);
    void MakeFileData(uint32_t dataLength, std::string& answer);
    void MakeData(uint32_t dataLength, std::string& answer);
    void CheckSeek(uint32_t stepLen, uint32_t bufferSize, const std::string& answer);
    void CheckFileReader(BufferedFileWrapper& reader, const std::string& answer);
    void CheckAnswer(const std::string& answer);

protected:
    bool mAsync;

private:
    std::string mFilePath;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_BUFFEREDFILEWRAPPERTEST_H
