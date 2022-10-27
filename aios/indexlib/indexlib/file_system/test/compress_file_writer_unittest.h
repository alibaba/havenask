#ifndef __INDEXLIB_COMPRESSFILEWRITERTEST_H
#define __INDEXLIB_COMPRESSFILEWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/compress_file_writer.h"
#include "indexlib/file_system/compress_file_reader.h"

IE_NAMESPACE_BEGIN(file_system);

class CompressFileWriterTest : public INDEXLIB_TESTBASE
{
public:
    CompressFileWriterTest();
    ~CompressFileWriterTest();

    DECLARE_CLASS_NAME(CompressFileWriterTest);
    
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCompressAddressMapperMemoryStatistic();

private:
    void InnerTestSimpleProcess(bool isMemDirectory);
    std::string MakeOriginData(size_t dataLen);

    void CheckData(const std::string &oriDataStr,
                   const CompressFileReaderPtr& compressReader,
                   size_t readStep);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CompressFileWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(CompressFileWriterTest, TestCompressAddressMapperMemoryStatistic);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_COMPRESSFILEWRITERTEST_H
