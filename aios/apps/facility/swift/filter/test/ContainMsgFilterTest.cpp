#include "swift/filter/ContainMsgFilter.h"

#include <cstddef>
#include <string>
#include <vector>

#include "autil/TimeUtility.h"
#include "swift/common/Common.h"
#include "swift/common/FieldGroupReader.h"
#include "swift/common/FieldGroupWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using namespace swift::common;

namespace swift {
namespace filter {

class ContainMsgFilterTest : public TESTBASE {};

TEST_F(ContainMsgFilterTest, testContainMsgFilterInit) {
    {
        ContainMsgFilter filter("", "a|b|c");
        EXPECT_FALSE(filter.init());
    }
    {
        ContainMsgFilter filter("name", "");
        EXPECT_FALSE(filter.init());
    }
    {
        ContainMsgFilter filter("name", "| |");
        EXPECT_FALSE(filter.init());
    }
    {
        ContainMsgFilter filter("name", " |a | ");
        EXPECT_TRUE(filter.init());
        EXPECT_EQ(1, filter.getValuesSearch().size());
        EXPECT_EQ("a", filter.getValuesSearch()[0]);
    }
}

TEST_F(ContainMsgFilterTest, testContainFilterMsg) {
    FieldGroupWriter fieldGroupWriter;
    string name1 = string("name");
    string value1 = string("a b c");
    fieldGroupWriter.addProductionField(name1, value1, true);
    string name2 = string("type");
    string value2 = string("1 3 5");
    fieldGroupWriter.addProductionField(name2, value2, false);
    string data;
    fieldGroupWriter.toString(data);
    FieldGroupReader fieldGroupReader;
    EXPECT_TRUE(fieldGroupReader.fromProductionString(data));
    EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
    {
        ContainMsgFilter filter("name", "c");
        EXPECT_TRUE(filter.init());
        EXPECT_TRUE(filter.filterMsg(fieldGroupReader));
    }
    {
        ContainMsgFilter filter("name", "d");
        EXPECT_TRUE(filter.init());
        EXPECT_FALSE(filter.filterMsg(fieldGroupReader));
    }
    {
        ContainMsgFilter filter("null", "a");
        EXPECT_TRUE(filter.init());
        EXPECT_FALSE(filter.filterMsg(fieldGroupReader));
    }
}

TEST_F(ContainMsgFilterTest, testContainFilterMsgOR) {
    FieldGroupWriter fieldGroupWriter;
    string name1 = string("name");
    string value1 = string("a b c");
    fieldGroupWriter.addProductionField(name1, value1, true);
    string name2 = string("type");
    string value2 = string("1 3 5");
    fieldGroupWriter.addProductionField(name2, value2, false);
    string data;
    fieldGroupWriter.toString(data);
    FieldGroupReader fieldGroupReader;
    EXPECT_TRUE(fieldGroupReader.fromProductionString(data));
    EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
    {
        ContainMsgFilter filter("name", "||a||b|");
        EXPECT_TRUE(filter.init());
        EXPECT_EQ(2, filter.getValuesSearch().size());
        EXPECT_TRUE(filter.filterMsg(fieldGroupReader));
    }
    {
        ContainMsgFilter filter("name", "a|d");
        EXPECT_TRUE(filter.init());
        EXPECT_TRUE(filter.filterMsg(fieldGroupReader));
    }
    {
        ContainMsgFilter filter("name", "e|a|m");
        EXPECT_TRUE(filter.init());
        EXPECT_TRUE(filter.filterMsg(fieldGroupReader));
    }
    {
        ContainMsgFilter filter("name", "d|f");
        EXPECT_TRUE(filter.init());
        EXPECT_FALSE(filter.filterMsg(fieldGroupReader));
    }
}

} // namespace filter
} // namespace swift
