#include "indexlib/file_system/test/versioned_package_file_meta_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, VersionedPackageFileMetaTest);

VersionedPackageFileMetaTest::VersionedPackageFileMetaTest()
{
}

VersionedPackageFileMetaTest::~VersionedPackageFileMetaTest()
{
}

void VersionedPackageFileMetaTest::CaseSetUp()
{
}

void VersionedPackageFileMetaTest::CaseTearDown()
{
}

void VersionedPackageFileMetaTest::TestRecognize()
{
    vector<string> fileNames = {
        "package_file.__meta__.i9t1.0.__tmp__",
        "package_file.__meta__.i9t0.36",
        "package_file.__meta__.i9t0.37",
        "package_file.__data__.i9t0.0",
        "package_file.__data__.i9t0.1",
        "package_file.__data__.i9t1.0",
        "package_file.__data__.i9t1.1",
        "package_file.__data__.i9t1.1.__tmp__",
        "sub_segment"
    };
    string recoverMetaPath;
    {
        // normal -1
        set<string> dataFileSet;
        set<string> uselessMetaFileSet;
        VersionedPackageFileMeta::Recognize("i9t0", -1, fileNames,
                dataFileSet, uselessMetaFileSet, recoverMetaPath);
        ASSERT_EQ((set<string>{"package_file.__data__.i9t0.0", "package_file.__data__.i9t0.1"}), dataFileSet);
        ASSERT_EQ(set<string>{"package_file.__meta__.i9t0.36"}, uselessMetaFileSet);
        ASSERT_EQ("package_file.__meta__.i9t0.37", recoverMetaPath);
    }
    {
        // normal 36
        set<string> dataFileSet;
        set<string> uselessMetaFileSet;
        VersionedPackageFileMeta::Recognize("i9t0", 36, fileNames,
                dataFileSet, uselessMetaFileSet, recoverMetaPath);
        ASSERT_EQ((set<string>{"package_file.__data__.i9t0.0", "package_file.__data__.i9t0.1"}), dataFileSet);
        ASSERT_EQ(set<string>{"package_file.__meta__.i9t0.37"}, uselessMetaFileSet);
        ASSERT_EQ("package_file.__meta__.i9t0.36", recoverMetaPath);
    }
    {
        // tmp meta
        set<string> dataFileSet;
        set<string> uselessMetaFileSet;
        VersionedPackageFileMeta::Recognize("i9t1", -1, fileNames,
                dataFileSet, uselessMetaFileSet, recoverMetaPath);
        ASSERT_EQ((set<string>{"package_file.__data__.i9t1.0", "package_file.__data__.i9t1.1", "package_file.__data__.i9t1.1.__tmp__"}), dataFileSet);
        ASSERT_EQ(set<string>{"package_file.__meta__.i9t1.0.__tmp__"}, uselessMetaFileSet);
        ASSERT_EQ("", recoverMetaPath);
    }
}

IE_NAMESPACE_END(file_system);

