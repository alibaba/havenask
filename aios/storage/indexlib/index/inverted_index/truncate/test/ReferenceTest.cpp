#include "indexlib/index/inverted_index/truncate/DocInfo.h"
#include "indexlib/index/inverted_index/truncate/ReferenceTyped.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class ReferenceTest : public TESTBASE
{
public:
    void setUp() override;
    void tearDown() override;

private:
    std::unique_ptr<char[]> _buf;
    DocInfo* _docInfo;
};

void ReferenceTest::setUp()
{
    _buf = std::make_unique<char[]>(100);
    _docInfo = (DocInfo*)_buf.get();
}

void ReferenceTest::tearDown() { _buf.reset(); }

TEST_F(ReferenceTest, TestNotSupportNull)
{
    auto refer = std::make_unique<ReferenceTyped<int8_t>>(/*offset*/ 4, ft_int8, /*supportNull*/ false);
    refer->Set(/*val*/ 100, /*isNull*/ false, _docInfo);

    bool isNull = false;
    int8_t val = 0;
    refer->Get(_docInfo, val, isNull);
    ASSERT_EQ(100, val);
    ASSERT_FALSE(isNull);
    auto str = refer->GetStringValue(_docInfo);
    ASSERT_EQ(std::string("100"), str);

    // test base class
    ASSERT_EQ(4, refer->GetOffset());
    ASSERT_EQ(ft_int8, refer->GetFieldType());
}

TEST_F(ReferenceTest, TestIsNull)
{
    auto refer = std::make_unique<ReferenceTyped<int16_t>>(/*offset*/ 10, ft_int16, /*supportNull*/ true);
    refer->Set(/*val*/ 101, /*isNull*/ true, _docInfo);

    bool isNull = false;
    int16_t val = 0;
    refer->Get(_docInfo, val, isNull);
    ASSERT_EQ(0, val);
    ASSERT_TRUE(isNull);
    auto str = refer->GetStringValue(_docInfo);
    ASSERT_EQ(std::string("NULL"), str);

    // test base class
    ASSERT_EQ(10, refer->GetOffset());
    ASSERT_EQ(ft_int16, refer->GetFieldType());
}

TEST_F(ReferenceTest, TestIsNotNull)
{
    auto refer = std::make_unique<ReferenceTyped<int16_t>>(/*offset*/ 10, ft_int16, /*supportNull*/ true);
    refer->Set(/*val*/ 101, /*isNull*/ false, _docInfo);

    bool isNull = false;
    int16_t val = 0;
    refer->Get(_docInfo, val, isNull);
    ASSERT_EQ(101, val);
    ASSERT_FALSE(isNull);
    auto str = refer->GetStringValue(_docInfo);
    ASSERT_EQ(std::string("101"), str);

    // test base class
    ASSERT_EQ(10, refer->GetOffset());
    ASSERT_EQ(ft_int16, refer->GetFieldType());
}

} // namespace indexlib::index
