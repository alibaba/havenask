#include "suez/drc/LogRecord.h"

#include "suez/drc/LogRecordBuilder.h"
#include "unittest/unittest.h"

namespace suez {

class LogRecordTest : public TESTBASE {};

TEST_F(LogRecordTest, testEmpty) {
    LogRecord record;
    ASSERT_FALSE(record.init(0, ""));
    ASSERT_EQ(LT_UNKNOWN, record.getType());
}

TEST_F(LogRecordTest, testBuildAndParse) {
    LogRecordBuilder builder;
    builder.addField(autil::StringView("k1"), autil::StringView("v1"));
    LogRecord log;
    // no log type
    ASSERT_FALSE(builder.finalize(log));

    // add log type
    builder.addField(autil::StringView("CMD"), autil::StringView("add"));
    ASSERT_TRUE(builder.finalize(log));
    ASSERT_EQ(LT_ADD, log.getType());

    autil::StringView val;
    ASSERT_TRUE(log.getField(autil::StringView("k1"), val));
    ASSERT_EQ("v1", val.to_string());

    ASSERT_FALSE(log.getField(autil::StringView("k2"), val));
}

TEST_F(LogRecordTest, testParseAlterDoc) {
    LogRecordBuilder builder;
    builder.addField(autil::StringView("k1"), autil::StringView("v1"));
    builder.addField(autil::StringView("CMD"), autil::StringView("alter"));
    LogRecord log;
    ASSERT_TRUE(builder.finalize(log));
    ASSERT_EQ(LT_ALTER_TABLE, log.getType());
}

} // namespace suez
