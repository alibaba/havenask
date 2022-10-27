#ifndef ISEARCH_BS_BINARYSTRINGUTIL_H
#define ISEARCH_BS_BINARYSTRINGUTIL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/LongHashValue.h>

namespace build_service {
namespace builder {

class BinaryStringUtil
{
private:
    BinaryStringUtil(const BinaryStringUtil &);
    BinaryStringUtil& operator=(const BinaryStringUtil &);
public:
    template <typename T>
    static void toInvertString(T t, std::string &result);

    template <typename T>
    static void toString(T t, std::string &result);

private:
    template <typename T>
    static void floatingNumToString(T t, std::string &result);

private:
    BS_LOG_DECLARE();
};

template <typename T>
inline void BinaryStringUtil::toInvertString(T t, std::string &result) {
    t = ~t;
    toString(t, result);
}

template <typename T>
inline void BinaryStringUtil::toString(T t, std::string &result) {
    size_t n = sizeof(T);
    result.resize(n + 1, 0);
    if (t > 0) {
        result[0] = 1;
    } 
    if (t == 0) {
        result[0] = 1;
    }
    for (size_t i = 0; i < n; ++i) {
        result[i + 1] = *((char *)&t + n - i - 1);
    }
}

template <>
inline void BinaryStringUtil::toString(double t, std::string &result) {
    floatingNumToString<double>(t, result);
}

template <>
inline void BinaryStringUtil::toString(float t, std::string &result) {
    floatingNumToString<float>(t, result);
}

template <>
inline void BinaryStringUtil::toInvertString<double>(double t, std::string &result) {
    t = -t;
    floatingNumToString<double>(t, result);
}

template <>
inline void BinaryStringUtil::toInvertString<float>(float t, std::string &result) {
    t = -t;
    floatingNumToString<float>(t, result);
}

template <typename T>
inline void BinaryStringUtil::floatingNumToString(T t, std::string &result) {
    size_t n = sizeof(T);
    result.resize(n);
    for (size_t i = 0; i < n; ++i) {
         result[i] = *((char *)&t + n - i - 1);
    }
    if (unlikely(result[0] & 0x80)) {
        for (size_t i = 0; i < n; ++i) {
            result[i] = ~result[i];
        }
    } else {
        result[0] |= 0x80;
    }
}

}
}

#endif //ISEARCH_BS_BINARYSTRINGUTIL_H
