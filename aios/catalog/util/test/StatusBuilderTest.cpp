#include "catalog/util/StatusBuilder.h"

#include "unittest/unittest.h"

namespace catalog {

class StatusBuilderTest : public TESTBASE {};

TEST_F(StatusBuilderTest, testSimple) {
    Status status;
    StatusBuilder builder(&status);
    builder.setOk();
    ASSERT_EQ(Status::OK, status.code());
    ASSERT_EQ("", status.message());

    builder.build(Status::NOT_FOUND, "abc ", 123, " 456");
    ASSERT_EQ(Status::NOT_FOUND, status.code());
    ASSERT_EQ("abc 123 456", status.message());

    builder.build(Status::DUPLICATE_ENTITY);
    ASSERT_EQ(Status::DUPLICATE_ENTITY, status.code());
    ASSERT_EQ("DUPLICATE_ENTITY", status.message());
}

} // namespace catalog
