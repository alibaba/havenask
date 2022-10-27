#ifndef ISEARCH_BS_UTF8UTIL_H
#define ISEARCH_BS_UTF8UTIL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace util {

class UTF8Util
{
private:
    UTF8Util();
    ~UTF8Util();
    UTF8Util(const UTF8Util &);
    UTF8Util& operator=(const UTF8Util &);
public:
    static std::string getNextCharUTF8(const std::string& str, size_t start);
    static bool getNextCharUTF8(const char *str, size_t start, size_t end, size_t &len);
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(UTF8Util);

}
}

#endif //ISEARCH_BS_UTF8UTIL_H
