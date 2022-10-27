#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/path_util.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);

class PathUtilTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(PathUtilTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForGetFileName()
    {
        INDEXLIB_TEST_EQUAL("home", PathUtil::GetFileName("/aa/home"));
        INDEXLIB_TEST_EQUAL("", PathUtil::GetFileName("/aa/home/"));
        INDEXLIB_TEST_EQUAL("b", PathUtil::GetFileName("/aa/home/////b"));
    }

    void TestCaseForGetParentDirPath()
    {
        INDEXLIB_TEST_EQUAL("/home", PathUtil::GetParentDirPath("/home/aa"));
        INDEXLIB_TEST_EQUAL("/home", PathUtil::GetParentDirPath("/home/aa/"));
        INDEXLIB_TEST_EQUAL("/home", PathUtil::GetParentDirPath("/home/aa/////"));
        INDEXLIB_TEST_EQUAL("/home", PathUtil::GetParentDirPath("/home///aa"));
        INDEXLIB_TEST_EQUAL("/home", PathUtil::GetParentDirPath("/home///aa//"));
        INDEXLIB_TEST_EQUAL("home", PathUtil::GetParentDirPath("home///aa//"));
        INDEXLIB_TEST_EQUAL("/home/aa", PathUtil::GetParentDirPath("/home/aa/bb/"));
        INDEXLIB_TEST_EQUAL("", PathUtil::GetParentDirPath("/home/"));
        INDEXLIB_TEST_EQUAL("", PathUtil::GetParentDirPath("home"));
        INDEXLIB_TEST_EQUAL("pangu://home", PathUtil::GetParentDirPath("pangu://home/aa"));
        INDEXLIB_TEST_EQUAL("pangu:", PathUtil::GetParentDirPath("pangu://home/"));
    }

    void TestCaseForGetParentDirPathWithErrorPath()
    {
        INDEXLIB_TEST_EQUAL("", PathUtil::GetParentDirPath(""));
        INDEXLIB_TEST_EQUAL("", PathUtil::GetParentDirPath("/"));
        INDEXLIB_TEST_EQUAL("", PathUtil::GetParentDirPath("//"));
        INDEXLIB_TEST_EQUAL("", PathUtil::GetParentDirPath("\\"));
        INDEXLIB_TEST_EQUAL("", PathUtil::GetParentDirPath("pangu://"));
    }

    void TestCaseForGetParentDirName()
    {
        INDEXLIB_TEST_EQUAL("home", PathUtil::GetParentDirName("/home/aa"));
        INDEXLIB_TEST_EQUAL("home", PathUtil::GetParentDirName("/home/aa/"));
        INDEXLIB_TEST_EQUAL("home", PathUtil::GetParentDirName("/home/aa/////"));
        INDEXLIB_TEST_EQUAL("home", PathUtil::GetParentDirName("/home///aa"));
        INDEXLIB_TEST_EQUAL("home", PathUtil::GetParentDirName("/home///aa//"));
        INDEXLIB_TEST_EQUAL("home", PathUtil::GetParentDirName("home///aa//"));
        INDEXLIB_TEST_EQUAL("aa", PathUtil::GetParentDirName("/home/aa/bb/"));
    }

    void TestCaseForNormalizePath()
    {
        INDEXLIB_TEST_EQUAL("", PathUtil::NormalizePath(""));
        INDEXLIB_TEST_EQUAL("abc", PathUtil::NormalizePath("abc/"));
        INDEXLIB_TEST_EQUAL("abc", PathUtil::NormalizePath("abc//"));
        INDEXLIB_TEST_EQUAL("/abc", PathUtil::NormalizePath("//abc/"));
        INDEXLIB_TEST_EQUAL("/abc", PathUtil::NormalizePath("/abc//"));
        INDEXLIB_TEST_EQUAL("/", PathUtil::NormalizePath("/"));
        INDEXLIB_TEST_EQUAL("/", PathUtil::NormalizePath("//"));
        INDEXLIB_TEST_EQUAL("/home/aa", PathUtil::NormalizePath("/home/aa"));
        INDEXLIB_TEST_EQUAL("/home/aa", PathUtil::NormalizePath("/home/aa/"));
        INDEXLIB_TEST_EQUAL("/home/aa", PathUtil::NormalizePath("/home/aa/////"));
        INDEXLIB_TEST_EQUAL("/home/aa", PathUtil::NormalizePath("/home///aa"));
        INDEXLIB_TEST_EQUAL("/home/aa", PathUtil::NormalizePath("/home///aa//"));
        INDEXLIB_TEST_EQUAL("home/aa", PathUtil::NormalizePath("home///aa//"));
        INDEXLIB_TEST_EQUAL("/home/aa/bb", PathUtil::NormalizePath("/home/aa/bb/"));
        INDEXLIB_TEST_EQUAL("hdfs://home/aa", PathUtil::NormalizePath("hdfs://home/aa"));
        INDEXLIB_TEST_EQUAL("hdfs://home/aa", PathUtil::NormalizePath("hdfs:///home//aa/"));
        INDEXLIB_TEST_EQUAL("hdfs://home/", PathUtil::NormalizePath("hdfs://home//"));
        INDEXLIB_TEST_EQUAL("hdfs://home/", PathUtil::NormalizePath("hdfs://home///"));
    }

    void TestCaseForIsInRootPath()
    {
        INDEXLIB_TEST_TRUE(PathUtil::IsInRootPath("/home/aa", "/home"));
        INDEXLIB_TEST_TRUE(PathUtil::IsInRootPath("/home/aa", "/home/aa"));
        INDEXLIB_TEST_TRUE(PathUtil::IsInRootPath("/home/aa/b", "/home/aa"));
        ASSERT_FALSE(PathUtil::IsInRootPath("/home/aa", "/home/aaa"));
        ASSERT_FALSE(PathUtil::IsInRootPath("/home/aa", "/home/a"));
        ASSERT_FALSE(PathUtil::IsInRootPath("/home/b", "/home/a"));
    }

    void TestSetFsConfig()
    {
        ASSERT_EQ("/ab/c/d", PathUtil::SetFsConfig("/ab/c/d", "conf=1"));
        ASSERT_EQ("hdfs://a", PathUtil::SetFsConfig("hdfs://a", "conf=1"));
        ASSERT_EQ("hdfs://", PathUtil::SetFsConfig("hdfs://", "conf=1"));
        ASSERT_EQ("", PathUtil::SetFsConfig("", "conf=1"));
        ASSERT_EQ("hdfs://a?conf=1/b/c", PathUtil::SetFsConfig("hdfs://a/b/c", "conf=1"));
        ASSERT_EQ("hdfs://a?conf=1/", PathUtil::SetFsConfig("hdfs://a/", "conf=1"));
        ASSERT_EQ("hdfs://a?conf=1/b/", PathUtil::SetFsConfig("hdfs://a/b/", "conf=1"));
        ASSERT_EQ("hdfs://a/b/c", PathUtil::SetFsConfig("hdfs://a/b/c", ""));
    }

    void TestGetRelativePath()
    {
        ASSERT_EQ("/a/b/", PathUtil::GetRelativePath("/a/b/"));
        ASSERT_EQ("a/b/c", PathUtil::GetRelativePath("a/b/c"));
        ASSERT_EQ("/a/b/", PathUtil::GetRelativePath("hdfs://xxx/a/b/"));
        ASSERT_EQ("/a/b/c", PathUtil::GetRelativePath("hdfs://xxx?a=1/a/b/c"));
        ASSERT_EQ("/a/b/c", PathUtil::GetRelativePath("zfs://xxx?a=1/a/b/c"));
    }

    void TestAddFsConfig() {
        ASSERT_EQ("/ab/c/d", PathUtil::AddFsConfig("/ab/c/d", "conf=1"));
        ASSERT_EQ("hdfs://a", PathUtil::AddFsConfig("hdfs://a", "conf=1"));
        ASSERT_EQ("hdfs://", PathUtil::AddFsConfig("hdfs://", "conf=1"));
        ASSERT_EQ("", PathUtil::AddFsConfig("", "conf=1"));
        ASSERT_EQ("hdfs://a?conf=1/b/c", PathUtil::AddFsConfig("hdfs://a/b/c", "conf=1"));
        ASSERT_EQ("hdfs://a?conf=1/", PathUtil::AddFsConfig("hdfs://a/", "conf=1"));
        ASSERT_EQ("hdfs://a?conf=1/b/", PathUtil::AddFsConfig("hdfs://a/b/", "conf=1"));
        ASSERT_EQ("hdfs://a/b/c", PathUtil::AddFsConfig("hdfs://a/b/c", ""));

        ASSERT_EQ("?a=3/ab/c/d", PathUtil::AddFsConfig("?a=3/ab/c/d", "conf=1"));
        ASSERT_EQ("hdfs://a?a=3", PathUtil::AddFsConfig("hdfs://a?a=3", "conf=1"));
        ASSERT_EQ("hdfs://?a=3", PathUtil::AddFsConfig("hdfs://?a=3", "conf=1"));
        ASSERT_EQ("?", PathUtil::AddFsConfig("?", "conf=1"));
        ASSERT_EQ("hdfs://a?a=3&conf=1/b/c", PathUtil::AddFsConfig("hdfs://a?a=3/b/c", "conf=1"));
        ASSERT_EQ("hdfs://a?a=3&conf=1/", PathUtil::AddFsConfig("hdfs://a?a=3/", "conf=1"));
        ASSERT_EQ("hdfs://a?a=3&conf=1/b/", PathUtil::AddFsConfig("hdfs://a?a=3/b/", "conf=1"));
        ASSERT_EQ("hdfs://a?a=3&b=4/b/c", PathUtil::AddFsConfig("hdfs://a?a=3&b=4/b/c", ""));                
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, PathUtilTest);

INDEXLIB_UNIT_TEST_CASE(PathUtilTest, TestCaseForGetParentDirPath);
INDEXLIB_UNIT_TEST_CASE(PathUtilTest, TestCaseForGetParentDirPathWithErrorPath);
INDEXLIB_UNIT_TEST_CASE(PathUtilTest, TestCaseForGetParentDirName);
INDEXLIB_UNIT_TEST_CASE(PathUtilTest, TestCaseForNormalizePath);
INDEXLIB_UNIT_TEST_CASE(PathUtilTest, TestCaseForGetFileName);
INDEXLIB_UNIT_TEST_CASE(PathUtilTest, TestCaseForIsInRootPath);
INDEXLIB_UNIT_TEST_CASE(PathUtilTest, TestSetFsConfig);
INDEXLIB_UNIT_TEST_CASE(PathUtilTest, TestGetRelativePath);
INDEXLIB_UNIT_TEST_CASE(PathUtilTest, TestAddFsConfig);

IE_NAMESPACE_END(util);
