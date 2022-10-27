/**
 * File name: PinYinTransTest.cpp
 * Author: zhixu.zt
 * Create time: 2014-01-05 16:57:51
 */

#include <string>
#include <vector>
#include <gtest/gtest.h>
#include "build_service/test/unittest.h"
#include "build_service/common_define.h"
#include "build_service/util/PinYinTrans.h"

BS_NAMESPACE_USE(util);

class PinYinTransTest : public testing::Test {
 public:
    virtual void SetUp();
    virtual void TearDown();
};

void PinYinTransTest::SetUp() {}

void PinYinTransTest::TearDown() {}

TEST_F(PinYinTransTest, PinyinTransTest) {
    PinYinTrans pinyin;
    char sz_file[PATH_MAX];
    snprintf(sz_file, PATH_MAX, "%s/util_test/pinyin.txt", TEST_DATA_PATH);
    static const std::string sz_test = "丂";
    ASSERT_EQ(pinyin.init(sz_file), 0);

    std::vector<std::string> res;

    pinyin.getQuanPin(sz_test, res);
    EXPECT_EQ(res.size(), 3u);
    EXPECT_EQ(res[0], std::string("kao"));
    EXPECT_EQ(res[1], std::string("qiao"));
    EXPECT_EQ(res[2], std::string("yu"));

    res.clear();
    pinyin.getJianPin(sz_test, res);
    EXPECT_EQ(res.size(), 3u);
    EXPECT_EQ(res[0], std::string("k"));
    EXPECT_EQ(res[1], std::string("q"));
    EXPECT_EQ(res[2], std::string("y"));
}

TEST_F(PinYinTransTest, AbnormalPinyinTransTest) {
    PinYinTrans pinyin;
    char sz_file[PATH_MAX];
    snprintf(sz_file, PATH_MAX, "%s/util_test/pinyin.txt", TEST_DATA_PATH);
    // 这个query曾经把内存撑爆了.
    static const std::string sz_test =
        "可爱的小海星造型吸嘴,柔软地将阴道口紧紧吸吮住,有如男性的口交。配强而有力的变频震荡,"
        "能让人感受有如男性的舔、含、吮吸一般,让淫水涌流不止。  "
        "非凡的设计.完美的外观.给人一种可爱而高尚的情趣.无线变频震动.游走于女性阴唇.带来无限刺激."
        "完美 情趣.  每一次穿戴都有万般呵护.犹如万般宠爱的公主!  使用方法:   "
        "1:装好电池(本品配用三枚钮扣电池无需要购买)   2打开小海 "
        "星的系带,固定于大腿根部,将海星紧贴阴道口 3打开轻触开关,享受变频震荡的刺激。体验万般感觉. "
        "规格:长:10cm 宽:10cm  注意事 项:  "
        "本器具仅供个人或情侣夫妻调情使用,使用时应注意清洁卫生。本器具使用后清洗时,"
        "电路部分切勿接触水,"
        "以免出现电路故障。外套切勿与油墨等易污物接触。收藏前应从电池盒内取出电池,器具装入包装盒内,"
        "应按原位置分别放置收藏好以备下次使用。  详细说明:功能及特点:100% 硅胶、防水 、7 "
        "段脉冲震动、新颖的穿戴式装饰 新颖穿戴式性爱小海星是您最佳的隐蔽小情人!! "
        "海星上的小突起在震动时能给女性外阴带来更强大的摩擦感,让性爱的高潮感觉好像热浪一样,"
        "一波一波的向你涌过来。 让你快乐每一天 燃烧你的欲望之火!!!随时随地!!!";
    ASSERT_EQ(pinyin.init(sz_file), 0);

    std::vector<std::string> res;

    pinyin.getQuanPin(sz_test, res);
    EXPECT_EQ(res.size(), 16u);
    //    EXPECT_EQ(res[0], std::string("kao"));
    //    EXPECT_EQ(res[1], std::string("qiao"));
    //    EXPECT_EQ(res[2], std::string("yu"));

    res.clear();
    pinyin.getJianPin(sz_test, res);
    EXPECT_EQ(res.size(), 16u);

    res.clear();
    pinyin.getJianPin(sz_test, res, true);
    EXPECT_EQ(res.size(), 1u);

    //    EXPECT_EQ(res[0], std::string("k"));
    //    EXPECT_EQ(res[1], std::string("q"));
    //    EXPECT_EQ(res[2], std::string("y"));
}
