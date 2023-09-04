#include "indexlib/common_define.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class IndexLocatorTest : public INDEXLIB_TESTBASE
{
};

TEST_F(IndexLocatorTest, SerializeAndDeserialize)
{
    IndexLocator locator1(/*src=*/123, /*offset=*/456, /*userData=*/"test");
    EXPECT_EQ(123, locator1.getSrc());
    EXPECT_EQ(456, locator1.getOffset());
    EXPECT_EQ(20, locator1.size());
    EXPECT_EQ("test", locator1.getUserData());
    EXPECT_EQ(1, locator1.getProgress().size());
    EXPECT_EQ(0, locator1.getProgress()[0].from);
    EXPECT_EQ(65535, locator1.getProgress()[0].to);
    EXPECT_EQ(456, locator1.getProgress()[0].offset.first);

    IndexLocator locator2;
    std::string serialized = locator1.toString();
    EXPECT_EQ(20, serialized.size());

    EXPECT_TRUE(locator2.fromString(serialized));
    EXPECT_EQ(123, locator2.getSrc());
    EXPECT_EQ(456, locator2.getOffset());
    EXPECT_EQ(20, locator2.size());
    EXPECT_EQ("test", locator2.getUserData());
    EXPECT_EQ(1, locator2.getProgress().size());
    EXPECT_EQ(0, locator2.getProgress()[0].from);
    EXPECT_EQ(65535, locator2.getProgress()[0].to);
    EXPECT_EQ(456, locator2.getProgress()[0].offset.first);

    IndexLocator locator3;
    EXPECT_EQ(1, locator3.getProgress().size());
    EXPECT_EQ(0, locator3.getProgress()[0].from);
    EXPECT_EQ(65535, locator3.getProgress()[0].to);
    EXPECT_EQ(locator3.getOffset(), locator3.getProgress()[0].offset.first);

    IndexLocator locator4;
    locator4.setOffset(100);
    EXPECT_EQ(1, locator4.getProgress().size());
    EXPECT_EQ(0, locator4.getProgress()[0].from);
    EXPECT_EQ(65535, locator4.getProgress()[0].to);
    EXPECT_EQ(100, locator4.getProgress()[0].offset.first);
}

TEST_F(IndexLocatorTest, TestCompatable)
{
    indexlib::document::IndexLocator oldLocator(123, 456, "");
    indexlibv2::framework::Locator newLocator;
    ASSERT_TRUE(newLocator.Deserialize(oldLocator.toString()));
    ASSERT_EQ(123, newLocator.GetSrc());
    ASSERT_EQ(456, newLocator.GetOffset().first);
    ASSERT_EQ(1, newLocator.GetProgress().size());
    ASSERT_EQ(indexlibv2::base::Progress(0, 65535, {456, 0}), newLocator.GetProgress()[0]);
}

}} // namespace indexlib::document
