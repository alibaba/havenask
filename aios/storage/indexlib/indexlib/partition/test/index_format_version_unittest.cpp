#include "indexlib/partition/test/index_format_version_unittest.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexFormatVersionTest);

IndexFormatVersionTest::IndexFormatVersionTest() {}

IndexFormatVersionTest::~IndexFormatVersionTest() {}

void IndexFormatVersionTest::CaseSetUp() { mRootDir = GET_TEMP_DATA_PATH(); }

void IndexFormatVersionTest::CaseTearDown() {}

void IndexFormatVersionTest::TestSimpleProcess()
{
    string filePath = util::PathUtil::JoinPath(mRootDir, "index_format_version");
    IndexFormatVersion indexFormatVersion;
    indexFormatVersion.Set("1.8.4");
    indexFormatVersion.Store(filePath, FenceContext::NoFence());

    IndexFormatVersion indexFormatVersion2;
    indexFormatVersion2.Load(filePath);
    ASSERT_TRUE(indexFormatVersion2 == indexFormatVersion);
}

void IndexFormatVersionTest::TestCheckCompatible()
{
    IndexFormatVersion v1;
    v1.Set("3.3.3");
    ASSERT_NO_THROW(v1.CheckCompatible("3.3.3"));
    ASSERT_NO_THROW(v1.CheckCompatible("3.4.3"));
    ASSERT_NO_THROW(v1.CheckCompatible("3.3.9"));

    v1.Set("3.c.3");
    ASSERT_THROW(v1.CheckCompatible("3.3.3"), IndexCollapsedException);

    v1.Set("3.3.3");
    ASSERT_THROW(v1.CheckCompatible("3.2.3"), IndexCollapsedException);
    ASSERT_THROW(v1.CheckCompatible("3.5.3"), IndexCollapsedException);
    ASSERT_THROW(v1.CheckCompatible("4.3.3"), IndexCollapsedException);

    v1.Set("3.3");
    ASSERT_THROW(v1.CheckCompatible("3.3.3"), IndexCollapsedException);
}

void IndexFormatVersionTest::TestLoadAndStore()
{
    // test load
    IndexFormatVersion v1;
    string fakePath = util::PathUtil::JoinPath(mRootDir, "dummy_index_version");
    ASSERT_THROW(v1.Load(fakePath), NonExistException);

    string filePath = util::PathUtil::JoinPath(mRootDir, "broken_file_version");
    FslibWrapper::AtomicStoreE(filePath, "");
    ASSERT_THROW(v1.Load(filePath), IndexCollapsedException);

    // test store
    v1.Set("3.3.3");
    ASSERT_THROW(v1.Store(filePath, FenceContext::NoFence()), ExistException);
}

void IndexFormatVersionTest::TestCompareOperator()
{
    {
        IndexFormatVersion v1("2.1.0");
        IndexFormatVersion v2("2.1.2");
        EXPECT_TRUE(v1 < v2);
        EXPECT_TRUE(v1 <= v2);
        EXPECT_TRUE(v2 > v1);
    }
    {
        IndexFormatVersion v1("3.1.0");
        IndexFormatVersion v2("2.1.2");
        EXPECT_FALSE(v1 < v2);
        EXPECT_FALSE(v2 > v1);
    }
    {
        IndexFormatVersion v1("2.1.0");
        IndexFormatVersion v2("2.1.0");
        EXPECT_FALSE(v1 < v2);
        EXPECT_FALSE(v2 > v1);
        EXPECT_TRUE(v2 == v1);
    }
    {
        IndexFormatVersion v1("2.1.0");
        IndexFormatVersion v2("2.2.0");
        EXPECT_TRUE(v1 < v2);
        EXPECT_TRUE(v2 > v1);
        EXPECT_TRUE(v2 >= v1);
        EXPECT_TRUE(v2 != v1);
    }

    {
        IndexFormatVersion v1("2.1.4");
        IndexFormatVersion v2("2.2.0");
        EXPECT_TRUE(v1 < v2);
        EXPECT_TRUE(v2 > v1);
        EXPECT_TRUE(v2 >= v1);
        EXPECT_TRUE(v1 <= v2);
        EXPECT_TRUE(v2 != v1);
    }
}
}} // namespace indexlib::partition
