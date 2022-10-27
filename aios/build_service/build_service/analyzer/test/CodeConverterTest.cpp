/**
 * File name: CodeConverterTest.cpp
 * Author: zhixu.zt
 * Create time: 2014-01-05 16:57:51
 */

#include <string>
#include <gtest/gtest.h>
#include "build_service/common_define.h"
#include "build_service/analyzer/CodeConverter.h"

using namespace std;
BS_NAMESPACE_USE(analyzer);

class CodeConverterTest : public testing::Test {
 public:
    virtual void SetUp();
    virtual void TearDown();
};

void CodeConverterTest::SetUp() {}

void CodeConverterTest::TearDown() {}

TEST_F(CodeConverterTest, testUnicodeToUTF8) {
    {
        string str("\\u5B9D\\u7801");  // 宝码
        uint16_t src[128];
        uint32_t index = 0;
        for (size_t i = 0; i < str.size() && index < sizeof(src);) {
            if (str[i] == '\\' && str[i + 1] == 'u') {
                char temp[5];
                memcpy(temp, &(str[i + 2]), 4);
                temp[4] = 0;
                src[index] = strtoul(temp, NULL, 16);
                i += 6;
                ++index;
            } else {
                src[index] = str[i];
                i += 1;
                ++index;
            }
        }
        char buffer[256];
        int len = CodeConverter::convertUnicodeToUTF8((const uint16_t *)src, index, buffer, sizeof(buffer));
        ASSERT_EQ(6, len);
        ASSERT_EQ("宝码", string(buffer, len));
    }
    {
        string str("\\u5B9D\\u7801123\\u5B9D");  // 宝码123宝
        uint16_t src[128];
        uint32_t index = 0;
        for (size_t i = 0; i < str.size() && index < sizeof(src);) {
            if (str[i] == '\\' && str[i + 1] == 'u') {
                char temp[5];
                memcpy(temp, &(str[i + 2]), 4);
                temp[4] = 0;
                src[index] = strtoul(temp, NULL, 16);
                i += 6;
                ++index;
            } else {
                src[index] = str[i];
                i += 1;
                ++index;
            }
        }
        char buffer[256];
        int len = CodeConverter::convertUnicodeToUTF8((const uint16_t *)src, index, buffer, sizeof(buffer));
        ASSERT_EQ(12, len);
        ASSERT_EQ("宝码123宝", string(buffer, len));
    }
}
