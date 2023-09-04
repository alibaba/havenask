#include "indexlib/framework/TabletId.h"

#include "unittest/unittest.h"

namespace indexlib { namespace framework {

class TabletIdTest : public TESTBASE
{
};

TEST_F(TabletIdTest, TestGetTabletName)
{
    TabletId tid("test_table", 123456, 0, 32767);
    ASSERT_EQ("test_table/123456/0_32767", tid.GenerateTabletName());
}

}} // namespace indexlib::framework
