#include "indexlib/index/attribute/format/SingleValueAttributeReadOnlyFormatter.h"

#include "autil/StringUtil.h"
#include "indexlib/config/CompressTypeOption.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlibv2::index {

class SingleValueAttributeReadOnlyFormatterTestTest : public TESTBASE
{
public:
    SingleValueAttributeReadOnlyFormatterTestTest() {}
    ~SingleValueAttributeReadOnlyFormatterTestTest() {}

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(SingleValueAttributeReadOnlyFormatterTestTest, TestCaseForSimple)
{
    {
        SingleValueAttributeReadOnlyFormatter<uint32_t> formatter(indexlib::config::CompressTypeOption(), false);
        ASSERT_TRUE(true);
    }
    {
        indexlib::config::CompressTypeOption compress;
        ASSERT_TRUE(compress.Init("fp16").IsOK());

        auto formatter =
            std::make_shared<SingleValueAttributeReadOnlyFormatter<float>>(compress, /*supportNull*/ false);
        ASSERT_TRUE(true);
    }
}

} // namespace indexlibv2::index
