#ifndef __INDEXLIB_VERSIONEDPACKAGEFILEMETATEST_H
#define __INDEXLIB_VERSIONEDPACKAGEFILEMETATEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/versioned_package_file_meta.h"

IE_NAMESPACE_BEGIN(file_system);

class VersionedPackageFileMetaTest : public INDEXLIB_TESTBASE
{
public:
    VersionedPackageFileMetaTest();
    ~VersionedPackageFileMetaTest();

    DECLARE_CLASS_NAME(VersionedPackageFileMetaTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRecognize();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VersionedPackageFileMetaTest, TestRecognize);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_VERSIONEDPACKAGEFILEMETATEST_H
