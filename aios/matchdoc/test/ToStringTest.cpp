#include "matchdoc/ToString.h"

#include "gtest/gtest.h"

namespace matchdoc {

class ToStringTest : public testing::Test {};

TEST_F(ToStringTest, testSimple) {
    ASSERT_TRUE(__detail::ToStringImpl<uint32_t>::value);
    ASSERT_EQ("32", __detail::ToStringImpl<uint32_t>::toString(32));
    ASSERT_TRUE(__detail::ToStringImpl<long long>::value);
    ASSERT_TRUE(__detail::ToStringImpl<long>::value);
    ASSERT_TRUE(__detail::ToStringImpl<std::string>::value);
    ASSERT_EQ("abcdef", __detail::ToStringImpl<std::string>::toString("abcdef"));
    ASSERT_TRUE(__detail::ToStringImpl<std::vector<long>>::value);
    ASSERT_EQ("123", __detail::ToStringImpl<std::vector<long>>::toString({123}));
    ASSERT_EQ("12-3", __detail::ToStringImpl<std::vector<long>>::toString({1, 2, -3}));
    ASSERT_TRUE(__detail::ToStringImpl<std::vector<std::vector<long>>>::value);
    ASSERT_TRUE(__detail::ToStringImpl<autil::MultiChar>::value);
    ASSERT_TRUE(__detail::ToStringImpl<autil::MultiString>::value);

    struct A {};
    ASSERT_FALSE(__detail::ToStringImpl<A>::value);
    ASSERT_EQ("", __detail::ToStringImpl<A>::toString(A()));

    struct B {
        std::string toString() const { return "B::toString"; }
    };
    ASSERT_TRUE(__detail::ToStringImpl<B>::value);
    ASSERT_EQ("B::toString", __detail::ToStringImpl<B>::toString(B()));

    struct C {
        static std::string toString(const C &c) { return "static C::toString"; }
    };
    ASSERT_TRUE(__detail::ToStringImpl<C>::value);
    ASSERT_EQ("static C::toString", __detail::ToStringImpl<C>::toString(C()));
}

} // namespace matchdoc
