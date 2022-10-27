#ifndef ISEARCH_BS_STRING_UTIL_H
#define ISEARCH_BS_STRING_UTIL_H

#include <stdint.h>
#include <limits.h>
#include <string>
#include <vector>
#include <set>

#define ISEARCH_BS_MAX_BOLD_TERMS 64
#define ISEARCH_BS_BOLD_BEGIN "<b>"
#define ISEARCH_BS_BOLD_END "</b>"

namespace build_service {
namespace util {

struct cn_result {
    std::string cn_str;
    bool first_flag;
};

class StringUtil {
 public:
    static char* trim(char* szStr);
    static std::string removeSpace(const std::string& str);

    static unsigned int split(std::vector<std::string>& v, const std::string& s, char delimiter,
                              unsigned int maxSegments = INT_MAX);
    template <typename T>
    static unsigned int split(T& output, const std::string& s, char delimiter, unsigned int maxSegments = INT_MAX);

    static std::string join(const std::vector<std::string>& array, const std::string& seperator);

    static bool strToKV32(const char* str, uint64_t& value);
    static bool strToKVFloat(const char* str, uint64_t& value);

    // 是否全是非中文字符
    static bool isAllChar(const std::string& str);
    // 是否包含非中文字符
    static bool containsChar(const std::string& str);
    // 是否是中文和英文的混合
    static bool isCnCharSemi(const std::string& str, cn_result& cn_obj);
    // 根据分词将src 字符串加粗
    static int32_t boldLineStr(const std::vector<std::string>& segments, const char* src, int32_t src_len, char* dst,
                               int32_t dest_len);

    static bool tryConvertToDateInMonth(int64_t inputTs, std::string& str);    
};

template <typename T>
inline unsigned int StringUtil::split(T& output, const std::string& s, char delimiter, unsigned int maxSegments) {
    std::string::size_type left = 0;
    unsigned int i = 0;
    unsigned int num = 0;
    for (i = 1; i < maxSegments; i++) {
        std::string::size_type right = s.find(delimiter, left);
        if (right == std::string::npos) {
            break;
        }
        *output++ = s.substr(left, right - left);
        left = right + 1;
        ++num;
    }
    std::string lastSubStr = s.substr(left);
    if (lastSubStr != "") {
        *output++ = lastSubStr;
        ++num;
    }
    return num;
}

}
}

#endif  // ISEARCH_BS_STRING_UTIL_H
