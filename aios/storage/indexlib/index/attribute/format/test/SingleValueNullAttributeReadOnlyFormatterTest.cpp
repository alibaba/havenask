#include "indexlib/index/attribute/format/SingleValueNullAttributeReadOnlyFormatter.h"

#include "autil/mem_pool/Pool.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class SingleValueNullAttributeReadOnlyFormatterTest : public TESTBASE
{
public:
    SingleValueNullAttributeReadOnlyFormatterTest() = default;
    ~SingleValueNullAttributeReadOnlyFormatterTest() = default;

public:
    void setUp() override {};
    void tearDown() override {};
};

TEST_F(SingleValueNullAttributeReadOnlyFormatterTest, TestSimpleProcess)
{
    auto formatter = std::make_shared<SingleValueNullAttributeReadOnlyFormatter<float>>();
    formatter->Init(indexlib::config::CompressTypeOption());
    ASSERT_TRUE(true);
}

} // namespace indexlibv2::index
