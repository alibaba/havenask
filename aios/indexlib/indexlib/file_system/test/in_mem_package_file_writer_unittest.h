#ifndef __INDEXLIB_INMEMPACKAGEFILEWRITERTEST_H
#define __INDEXLIB_INMEMPACKAGEFILEWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/in_mem_package_file_writer.h"

IE_NAMESPACE_BEGIN(file_system);

class InMemPackageFileWriterTest : public INDEXLIB_TESTBASE
{
public:
    InMemPackageFileWriterTest();
    ~InMemPackageFileWriterTest();

    DECLARE_CLASS_NAME(InMemPackageFileWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCloseException();
    void TestCreateInnerFileWriter();
    void TestCreateInnerFileWithDuplicateFilePath();
    void TestCreateEmptyPackage();

private:
    void WriteData(const PackageFileWriterPtr& writer, const std::string& fileName,
                   const std::string& value);

    void CheckData(const InMemStoragePtr& inMemStorage,
                   const std::string& filePath,
                   const std::string& expectValue);

private:
    util::BlockMemoryQuotaControllerPtr mMemoryController;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemPackageFileWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(InMemPackageFileWriterTest, TestCloseException);
INDEXLIB_UNIT_TEST_CASE(InMemPackageFileWriterTest, TestCreateInnerFileWriter);
INDEXLIB_UNIT_TEST_CASE(InMemPackageFileWriterTest, TestCreateInnerFileWithDuplicateFilePath);
INDEXLIB_UNIT_TEST_CASE(InMemPackageFileWriterTest, TestCreateEmptyPackage);


IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INMEMPACKAGEFILEWRITERTEST_H
