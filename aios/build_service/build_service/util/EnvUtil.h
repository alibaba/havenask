#ifndef ISEARCH_BS_ENVUTIL_H
#define ISEARCH_BS_ENVUTIL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/StringUtil.h>

namespace build_service {
namespace util {

class EnvUtil
{
public:
    EnvUtil() {}
    ~EnvUtil() {}
private:
    EnvUtil(const EnvUtil &);
    EnvUtil& operator=(const EnvUtil &);
public:
    template <typename T>
    static bool getValueFromEnv(const char *key, T &value);
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(EnvUtil);

template <typename T>
bool EnvUtil::getValueFromEnv(const char *key, T &value) {
    char *v = getenv(key);
    if (v == NULL) {
        return false;
    }
    
    T tmp;
    if (!autil::StringUtil::fromString(std::string(v), tmp)) {
        return false;
    }
    
    value = tmp;
    return true;
}

}
}

#endif //ISEARCH_BS_ENVUTIL_H
