#include "swift/filter/InMsgFilter.h"

#include <cstddef>
#include <string>

#include "autil/Span.h"
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

class InMsgFilterTest : public TESTBASE {};

TEST_F(InMsgFilterTest, testFilterMsg) {
    FieldGroupWriter fieldGroupWriter;
    string name1 = string("name");
    string value1 = string("a");
    fieldGroupWriter.addProductionField(name1, value1, true);
    string name2 = string("type");
    string value2 = string("1");
    fieldGroupWriter.addProductionField(name2, value2, false);
    string data;
    fieldGroupWriter.toString(data);
    FieldGroupReader fieldGroupReader;
    EXPECT_TRUE(fieldGroupReader.fromProductionString(data));
    EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
    {
        InMsgFilter filter("name", "a|b");
        EXPECT_TRUE(filter.init());
        EXPECT_TRUE(filter.filterMsg(fieldGroupReader));
    }
    {
        InMsgFilter filter("type", "1|2|3");
        EXPECT_TRUE(filter.init());
        EXPECT_TRUE(filter.filterMsg(fieldGroupReader));
    }
    {
        InMsgFilter filter("null", "a|b");
        EXPECT_TRUE(filter.init());
        EXPECT_FALSE(filter.filterMsg(fieldGroupReader));
    }
    {
        InMsgFilter filter("name", "2");
        EXPECT_TRUE(filter.init());
        EXPECT_FALSE(filter.filterMsg(fieldGroupReader));
    }
}
TEST_F(InMsgFilterTest, testOptionalField) {
    FieldGroupWriter fieldGroupWriter;
    string name1 = string("name");
    string value1 = string("a");
    fieldGroupWriter.addProductionField(name1, value1, true);
    string name2 = string("type");
    string value2 = string("1");
    fieldGroupWriter.addProductionField(name2, value2, false);
    string data;
    fieldGroupWriter.toString(data);
    FieldGroupReader fieldGroupReader;
    EXPECT_TRUE(fieldGroupReader.fromProductionString(data));
    EXPECT_EQ((size_t)2, fieldGroupReader.getFieldSize());
    {
        InMsgFilter filter("[name", "a|b");
        EXPECT_TRUE(filter.init());
        EXPECT_FALSE(filter.filterMsg(fieldGroupReader));
    }

    {
        InMsgFilter filter("name]", "a|b");
        EXPECT_TRUE(filter.init());
        EXPECT_FALSE(filter.filterMsg(fieldGroupReader));
    }
    {
        InMsgFilter filter("[name]", "a|b");
        EXPECT_TRUE(filter.init());
        EXPECT_TRUE(filter.filterMsg(fieldGroupReader));
    }
    {
        InMsgFilter filter("name", "a|b");
        EXPECT_TRUE(filter.init());
        EXPECT_TRUE(filter.filterMsg(fieldGroupReader));
    }

    {
        InMsgFilter filter("[null]", "a|b");
        EXPECT_TRUE(filter.init());
        EXPECT_TRUE(filter.filterMsg(fieldGroupReader));
    }
}

TEST_F(InMsgFilterTest, testInit) {
    {
        InMsgFilter filter("", "a|b|c");
        EXPECT_FALSE(filter.init());
    }
    {
        InMsgFilter filter("name", "");
        EXPECT_FALSE(filter.init());
    }
    {
        InMsgFilter filter("name", "|");
        EXPECT_FALSE(filter.init());
    }
    {
        InMsgFilter filter("name", "a");
        EXPECT_TRUE(filter.init());
        InMsgFilter::FieldsSet valSet = filter.getFilterSet();
        EXPECT_EQ((size_t)1, valSet.size());
        EXPECT_TRUE(valSet.count(StringView("a")) > 0);
    }
    {
        InMsgFilter filter("name", "a|b");
        EXPECT_TRUE(filter.init());
        InMsgFilter::FieldsSet valSet = filter.getFilterSet();
        EXPECT_EQ((size_t)2, valSet.size());
        EXPECT_TRUE(valSet.count(StringView("a")) > 0);
        EXPECT_TRUE(valSet.count(StringView("b")) > 0);
    }
    {
        InMsgFilter filter("name", "a||b");
        EXPECT_TRUE(filter.init());
        InMsgFilter::FieldsSet valSet = filter.getFilterSet();
        EXPECT_EQ((size_t)2, valSet.size());
        EXPECT_TRUE(valSet.count(StringView("a")) > 0);
        EXPECT_TRUE(valSet.count(StringView("b")) > 0);
    }
}

} // namespace filter
} // namespace swift
