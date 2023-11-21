#include "matchdoc/ReferenceSet.h"

#include "gtest/gtest.h"

namespace matchdoc {

class ReferenceSetTest : public testing::Test {
protected:
    template <typename T>
    ReferenceBase *make(const std::string &name) {
        auto ref = new Reference<T>();
        ref->setName(name);
        return ref;
    }
};

TEST_F(ReferenceSetTest, testRename) {
    ReferenceSet rs;
    ASSERT_FALSE(rs.rename("not_exist", "other"));

    rs.put(make<int32_t>("int32"));
    rs.put(make<int64_t>("int64"));
    rs.put(make<std::string>("string"));
    ASSERT_EQ(3, rs.size());

    ASSERT_TRUE(rs.rename("int32", "int32"));
    ASSERT_EQ(3, rs.size());
    ASSERT_TRUE(rs.get("int32"));

    ASSERT_TRUE(rs.rename("int32", "int"));
    ASSERT_EQ(3, rs.size());
    ASSERT_FALSE(rs.get("int32"));
    ASSERT_TRUE(rs.get("int"));

    ASSERT_FALSE(rs.rename("int64", "int"));
    ASSERT_EQ(3, rs.size());
    ASSERT_TRUE(rs.get("int64"));
    ASSERT_TRUE(rs.get("int"));
}

TEST_F(ReferenceSetTest, testRemove) {
    ReferenceSet rs;
    ASSERT_TRUE(rs.remove("not_exist"));

    rs.put(make<int32_t>("int32"));
    ASSERT_TRUE(rs.get("int32"));
    rs.put(make<int64_t>("int64"));
    ASSERT_TRUE(rs.get("int64"));
    rs.put(make<std::string>("string"));
    ASSERT_TRUE(rs.get("string"));

    ASSERT_EQ(3, rs.size());

    ASSERT_FALSE(rs.remove("not_exist"));
    ASSERT_EQ(3, rs.size());

    ASSERT_FALSE(rs.remove("int32"));
    ASSERT_FALSE(rs.get("int32"));
    ASSERT_EQ(2, rs.size());

    ASSERT_FALSE(rs.remove("int64"));
    ASSERT_FALSE(rs.get("int64"));
    ASSERT_EQ(1, rs.size());

    ASSERT_TRUE(rs.remove("string"));
    ASSERT_FALSE(rs.get("string"));
    ASSERT_EQ(0, rs.size());
}

} // namespace matchdoc
