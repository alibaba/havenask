#ifndef __INDEXLIB_PACKAGEFILEMETATEST_H
#define __INDEXLIB_PACKAGEFILEMETATEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/package_file_meta.h"

IE_NAMESPACE_BEGIN(file_system);

class PackageFileMetaTest : public INDEXLIB_TESTBASE
{
public:
    PackageFileMetaTest();
    ~PackageFileMetaTest();

    DECLARE_CLASS_NAME(PackageFileMetaTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInitFromFileNode();
    void TestInitFromString();
    void TestToString();
    void TestGetRelativeFilePath();
    void TestGetPackageDataFileLength();
    void TestGetPackageDataFileIdx();
private:
    void InnerTestInitFromFileNodes(const std::string& fileNodesDespStr,
                                    size_t fileAlignSize);
    void CheckInnerFileMeta(const std::string& fileNodesDespStr, 
                            const PackageFileMeta& packageFileMeta);
    std::vector<FileNodePtr> InitFileNodes(const std::string& fileNodesDespStr,
            const std::string& packageFilePath);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestInitFromFileNode);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestInitFromString);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestToString);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestGetRelativeFilePath);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestGetPackageDataFileLength);
INDEXLIB_UNIT_TEST_CASE(PackageFileMetaTest, TestGetPackageDataFileIdx);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKAGEFILEMETATEST_H
