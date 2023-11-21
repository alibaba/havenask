#include "matchdoc/ValueType.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <sys/time.h>

#include "autil/MultiValueCreator.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace matchdoc;

class ValueTypeTest : public testing::Test {};

TEST_F(ValueTypeTest, testSimple) {
    ValueType vt = ValueTypeHelper<int32_t>::getValueType();
    EXPECT_EQ(true, vt.needConstruct());
    ValueType vt2 = ValueTypeHelper<int32_t>::getValueType();
    EXPECT_TRUE(vt == vt2);
    EXPECT_TRUE(vt.getType() == vt2.getType());
    EXPECT_TRUE(vt.getTypeIgnoreConstruct() == vt2.getTypeIgnoreConstruct());

    vt.setNeedConstruct(false);
    EXPECT_EQ(false, vt.needConstruct());
    EXPECT_FALSE(vt == vt2);
    EXPECT_FALSE(vt.getType() == vt2.getType());
    EXPECT_TRUE(vt.getTypeIgnoreConstruct() == vt2.getTypeIgnoreConstruct());

    vt2.setNeedConstruct(false);
    EXPECT_EQ(false, vt.needConstruct());
    EXPECT_TRUE(vt == vt2);
    EXPECT_TRUE(vt.getType() == vt2.getType());
    EXPECT_TRUE(vt.getTypeIgnoreConstruct() == vt2.getTypeIgnoreConstruct());
}
