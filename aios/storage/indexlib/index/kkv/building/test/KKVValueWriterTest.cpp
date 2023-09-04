#include "indexlib/index/kkv/building/KKVValueWriter.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/util/SimplePool.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlibv2::index {

class KKVValueWriterTest : public TESTBASE
{
};

TEST_F(KKVValueWriterTest, TestSimpleProcess)
{
    {
        KKVValueWriter valueWriter(false);
        ASSERT_TRUE(valueWriter.Init(32 * 1024, 5).IsOK());
        ASSERT_GE(valueWriter.GetReserveBytes(), 32 * 1024 * 5);
        autil::StringView sv1("value1");
        autil::StringView sv2("value2");
        size_t offset1 = 0;
        ASSERT_TRUE(valueWriter.Append(sv1, offset1).IsOK());
        ASSERT_EQ(offset1, 0);
        ASSERT_EQ(valueWriter.GetUsedBytes(), sv1.size());
        size_t offset2 = 0;
        ASSERT_TRUE(valueWriter.Append(sv2, offset2).IsOK());
        ASSERT_EQ(offset2, sv1.size());
        ASSERT_EQ(valueWriter.GetUsedBytes(), sv1.size() + sv2.size());

        autil::StringView result1(valueWriter.GetValue(offset1), sv1.size());
        ASSERT_EQ(result1, sv1);
        autil::StringView result2(valueWriter.GetValue(offset2), sv2.size());
        ASSERT_EQ(result2, sv2);
    }

    // single slice
    {
        KKVValueWriter valueWriter(false);
        ASSERT_TRUE(valueWriter.Init(32 * 1024, 1).IsOK());
        ASSERT_GE(valueWriter.GetReserveBytes(), 32 * 1024 * 1);
        autil::StringView sv("value");
        size_t offset = 0;
        ASSERT_TRUE(valueWriter.Append(sv, offset).IsOK());
        ASSERT_EQ(offset, 0);
        ASSERT_EQ(valueWriter.GetUsedBytes(), sv.size());
        autil::StringView result(valueWriter.GetValue(offset), sv.size());
        ASSERT_EQ(result, sv);
    }
}

TEST_F(KKVValueWriterTest, TestWriteMulitSlice)
{
    KKVValueWriter valueWriter(false);
    size_t sliceSize = 8;
    ASSERT_TRUE(valueWriter.Init(sliceSize, 5).IsOK());
    ASSERT_GE(valueWriter.GetReserveBytes(), 8 * 5);

    autil::StringView sv1("abcdefg");
    autil::StringView sv2("higk");
    autil::StringView sv3("lm");
    autil::StringView sv4("1234567");

    size_t offset1 = 0;
    ASSERT_TRUE(valueWriter.Append(sv1, offset1).IsOK());
    ASSERT_EQ(offset1, 0);
    ASSERT_EQ(valueWriter.GetUsedBytes(), sv1.size());
    size_t offset2 = 0;
    ASSERT_TRUE(valueWriter.Append(sv2, offset2).IsOK());
    // has switch slice
    ASSERT_EQ(offset2, sliceSize);
    ASSERT_EQ(valueWriter.GetUsedBytes(), sliceSize + sv2.size());
    size_t offset3 = 0;
    ASSERT_TRUE(valueWriter.Append(sv3, offset3).IsOK());
    // not switch slice
    ASSERT_EQ(offset3, sliceSize + sv2.size());
    ASSERT_EQ(valueWriter.GetUsedBytes(), sliceSize + sv2.size() + sv3.size());
    size_t offset4 = 0;
    ASSERT_TRUE(valueWriter.Append(sv4, offset4).IsOK());
    // switch slice
    ASSERT_EQ(offset4, sliceSize * 2);
    ASSERT_EQ(valueWriter.GetUsedBytes(), sliceSize * 2 + sv4.size());

    autil::StringView result1(valueWriter.GetValue(offset1), sv1.size());
    ASSERT_EQ(result1, sv1);
    autil::StringView result2(valueWriter.GetValue(offset2), sv2.size());
    ASSERT_EQ(result2, sv2);
    autil::StringView result3(valueWriter.GetValue(offset3), sv3.size());
    ASSERT_EQ(result3, sv3);
    autil::StringView result4(valueWriter.GetValue(offset4), sv4.size());
    ASSERT_EQ(result4, sv4);
}

} // namespace indexlibv2::index
