#include <string>
#include <vector>
#include <gtest/gtest.h>
#include "build_service/common_define.h"
#include "build_service/util/StringUtil.h"

BS_NAMESPACE_USE(util);
using namespace std;

class StringUtilTest : public ::testing::Test {
 protected:
    StringUtilTest() {}
    virtual ~StringUtilTest() { }

    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(StringUtilTest, TestTrim) {
    char buf[16];
    memset(buf, 0, sizeof(buf));
    buf[0] = 0;
    char *ptr = StringUtil::trim(buf);
    ASSERT_EQ(ptr, buf);

    buf[0] = ' ';
    buf[1] = 'a';
    ptr = StringUtil::trim(buf);
    ASSERT_STREQ("a", ptr);

    buf[0] = 'a';
    buf[1] = ' ';
    ptr = StringUtil::trim(buf);
    ASSERT_STREQ("a", ptr);

    buf[0] = ' ';
    buf[1] = 'a';
    buf[2] = ' ';
    ptr = StringUtil::trim(buf);
    ASSERT_STREQ("a", ptr);
}

TEST_F(StringUtilTest, TestRemoveSpace) {
    string str = " hello,world\t";
    ASSERT_EQ("hello,world", StringUtil::removeSpace(str));
    str = " hello, \twor\tld\t";
    ASSERT_EQ("hello,world", StringUtil::removeSpace(str));
}

TEST_F(StringUtilTest, TestJoin) {
    vector<string> array;
    array.push_back("abc");
    ASSERT_EQ("abc", StringUtil::join(array, ","));
    array.push_back("def");
    ASSERT_EQ("abc,def", StringUtil::join(array, ","));
    array.push_back("hello");
    ASSERT_EQ("abc,def,hello", StringUtil::join(array, ","));
}

TEST_F(StringUtilTest, TestIsAllChar) {
    {  // noral string
        ASSERT_TRUE(StringUtil::isAllChar("dsfdsa"));
        ASSERT_TRUE(StringUtil::isAllChar("DEfds"));
        ASSERT_TRUE(StringUtil::isAllChar("42#rew#$"));
        ASSERT_TRUE(StringUtil::isAllChar("43"));
        ASSERT_TRUE(StringUtil::isAllChar("&())"));
        ASSERT_TRUE(StringUtil::isAllChar("\t"));
        ASSERT_TRUE(StringUtil::isAllChar("\""));
        ASSERT_TRUE(StringUtil::isAllChar(" "));
        ASSERT_TRUE(StringUtil::isAllChar("\0"));
        ASSERT_TRUE(StringUtil::isAllChar("dfsd"));
    }
    {  // 有中文字符
        ASSERT_FALSE(StringUtil::isAllChar("我是中文"));
        ASSERT_FALSE(StringUtil::isAllChar("woshi文"));
        ASSERT_FALSE(StringUtil::isAllChar("woshi｛｝、（）"));
    }
}

TEST_F(StringUtilTest, TestContaninsChar) {
    {  // 包含char
        ASSERT_TRUE(StringUtil::containsChar("dsfdsa"));
        ASSERT_TRUE(StringUtil::containsChar("DEfds"));
        ASSERT_TRUE(StringUtil::containsChar("42#rew#$"));
        ASSERT_TRUE(StringUtil::containsChar("43"));
        ASSERT_TRUE(StringUtil::containsChar("&())"));
        ASSERT_TRUE(StringUtil::containsChar("\t"));
        ASSERT_TRUE(StringUtil::containsChar("\""));
        ASSERT_TRUE(StringUtil::containsChar(" "));
        ASSERT_TRUE(StringUtil::containsChar("dfsd"));
        ASSERT_TRUE(StringUtil::containsChar("woshi文"));
        ASSERT_TRUE(StringUtil::containsChar("woshi｛｝、（）"));
    }
    {  // 全是中文字符
        ASSERT_FALSE(StringUtil::containsChar("我是中文"));
        ASSERT_FALSE(StringUtil::containsChar("（）＃！"));
        ASSERT_FALSE(StringUtil::containsChar("\0"));
    }
}

TEST_F(StringUtilTest, TestIsCnCharSemi) {
    // only "a"
    {
        cn_result cn_obj;
        ASSERT_FALSE(StringUtil::isCnCharSemi("a", cn_obj));
        ASSERT_EQ("", cn_obj.cn_str);
    }
    {
        cn_result cn_obj;
        ASSERT_FALSE(StringUtil::isCnCharSemi("adfs", cn_obj));
        ASSERT_EQ("", cn_obj.cn_str);
    }
    {
        cn_result cn_obj;
        ASSERT_FALSE(StringUtil::isCnCharSemi("", cn_obj));
    }
    {
        cn_result cn_obj;
        ASSERT_FALSE(StringUtil::isCnCharSemi("中国ni好", cn_obj));
        ASSERT_FALSE(cn_obj.first_flag);
        ASSERT_EQ("", cn_obj.cn_str);
    }
    {
        cn_result cn_obj;
        ASSERT_TRUE(StringUtil::isCnCharSemi("a中国好", cn_obj));
        ASSERT_TRUE(cn_obj.first_flag);
        ASSERT_EQ("中国好", cn_obj.cn_str);
    }
    {
        cn_result cn_obj;
        ASSERT_TRUE(StringUtil::isCnCharSemi("fdsaa中国好", cn_obj));
        ASSERT_TRUE(cn_obj.first_flag);
        ASSERT_EQ("中国好", cn_obj.cn_str);
    }
    {
        cn_result cn_obj;
        ASSERT_FALSE(StringUtil::isCnCharSemi("a中国好a", cn_obj));
        ASSERT_TRUE(cn_obj.first_flag);
        ASSERT_EQ("", cn_obj.cn_str);
    }
    {
        cn_result cn_obj;
        ASSERT_FALSE(StringUtil::isCnCharSemi("^中国好", cn_obj));
        ASSERT_FALSE(cn_obj.first_flag);
        ASSERT_EQ("", cn_obj.cn_str);
    }
    {
        cn_result cn_obj;
        ASSERT_FALSE(StringUtil::isCnCharSemi("^aad中国好", cn_obj));
        ASSERT_FALSE(cn_obj.first_flag);
        ASSERT_EQ("", cn_obj.cn_str);
    }
}

TEST_F(StringUtilTest, TestBoldLineStr) {
    {
        vector<string> segments;
        segments.push_back("luolinihao");
        segments.push_back("luoli");
        string src = "nihaoluoli 很好哦 ni 很好luoliluolinihenhao";
        char dest[1024];
        int32_t dest_len = sizeof(dest);
        uint32_t size = StringUtil::boldLineStr(segments, src.c_str(), src.length(), dest, dest_len);
        string expect_str =
            "<b>nihao</b>luoli <b>很好哦</b> <b>ni</b> "
            "<b>很好</b>luoliluoli<b>nihenhao</b>";
        ASSERT_EQ(expect_str, dest);
        ASSERT_EQ(expect_str.size(), size);
    }
    {
        vector<string> segments;
        segments.push_back("luolinihao");
        segments.push_back("luoli");
        string src = "";
        char dest[1024];
        int32_t dest_len = sizeof(dest);
        uint32_t size = StringUtil::boldLineStr(segments, src.c_str(), src.length(), dest, dest_len);
        string expect_str = "";
        ASSERT_EQ(expect_str, string(dest, size));
        ASSERT_EQ(expect_str.size(), size);
    }
}
