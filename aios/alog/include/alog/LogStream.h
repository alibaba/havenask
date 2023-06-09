/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _ALOG_LOGSTREAM_H_
#define _ALOG_LOGSTREAM_H_

#include <chrono>
#include <sstream>

#include "Logger.h"
namespace alog {
class LogStreamBuf : public std::streambuf {
public:
    LogStreamBuf(char *buf, int len) { setp(buf, buf + len - 2); }
    LogStreamBuf() {}
    int_type overflow(int_type ch) override { return ch; }

    char *dataStart() const { return std::streambuf::pbase(); }
    size_t dataLength() const { return static_cast<size_t>(pptr() - pbase()); }
};

class LogStream : public std::ostream {
public:
    LogStream(Logger *logger, uint32_t level, const char *file, int line, const char *func, int64_t ctr);
    ~LogStream();

    int64_t getCount() const { return _ctr; }
    void setCount(int64_t ctr) { _ctr = ctr; }
    LogStream *self() const { return _self; }

private:
    alog::Logger *_logger = nullptr;
    uint32_t _level = LOG_LEVEL_NOTSET;
    const char *_file = nullptr;
    int _line = 0;
    const char *_function = nullptr;
    int64_t _ctr = 0;
    LogStreamBuf _streambuf;
    LogStream *_self = nullptr;
};

class LogStreamVoidify {
public:
    LogStreamVoidify() {}
    void operator&(std::ostream &) {}
};

enum PRIVATE_Counter
{ COUNTER };
std::ostream &operator<<(std::ostream &os, const PRIVATE_Counter &);

struct CheckOpString {
    CheckOpString(std::string *str) : _str(str) {}
    ~CheckOpString() {
        if (nullptr != _str) {
            delete _str;
        }
    }
    operator bool() const { return __builtin_expect(_str != nullptr, 0); }
    std::string *_str = nullptr;
};

class CheckOpMessageBuilder {
public:
    explicit CheckOpMessageBuilder(const char *exprtext);
    ~CheckOpMessageBuilder();
    std::ostream *ForVar1() { return _stream; }
    std::ostream *ForVar2();
    std::string *NewString();

private:
    std::ostringstream *_stream = nullptr;
};

template <typename T>
inline void MakeCheckOpValueString(std::ostream *os, const T &v) {
    (*os) << v;
}
template <>
void MakeCheckOpValueString(std::ostream *os, const char &v);
template <>
void MakeCheckOpValueString(std::ostream *os, const signed char &v);
template <>
void MakeCheckOpValueString(std::ostream *os, const unsigned char &v);
template <>
void MakeCheckOpValueString(std::ostream *os, const std::nullptr_t &v);

template <typename T1, typename T2>
std::string *MakeCheckOpString(const T1 &v1, const T2 &v2, const char *exprtext) {
    alog::CheckOpMessageBuilder comb(exprtext);
    MakeCheckOpValueString(comb.ForVar1(), v1);
    MakeCheckOpValueString(comb.ForVar2(), v2);
    return comb.NewString();
}

#define DECLARE_CHECK_STROP_IMPL(func, expected)                                                                       \
    std::string *Check##func##expected##Impl(const char *s1, const char *s2, const char *names);
DECLARE_CHECK_STROP_IMPL(strcmp, true)
DECLARE_CHECK_STROP_IMPL(strcmp, false)
DECLARE_CHECK_STROP_IMPL(strcasecmp, true)
DECLARE_CHECK_STROP_IMPL(strcasecmp, false)
#undef DECLARE_CHECK_STROP_IMPL

#define INNER_CHECK_STROP(logger, level, func, op, expected, s1, s2)                                                   \
    if (CheckOpString _result = Check##func##expected##Impl((s1), (s2), #s1 " " #op " " #s2))                          \
    ALOG_STREAM(logger, level) << *(_result._str)

template <class T>
inline const T &GetReferenceableValue(const T &t) {
    return t;
}
#define INNER_CHECK_OP(logger, level, name, op, val1, val2)                                                            \
    if (CheckOpString _result =                                                                                        \
            Check##name##Impl(GetReferenceableValue(val1), GetReferenceableValue(val2), #val1 " " #op " " #val2))      \
    ALOG_STREAM(logger, level) << "Check failed: " << *(_result._str)

#define DEFINE_CHECK_OP_IMPL(name, op)                                                                                 \
    template <typename T1, typename T2>                                                                                \
    inline std::string *name##Impl(const T1 &v1, const T2 &v2, const char *exprtext) {                                 \
        if (__builtin_expect(!!(v1 op v2), 1)) {                                                                       \
            return NULL;                                                                                               \
        } else {                                                                                                       \
            return MakeCheckOpString(v1, v2, exprtext);                                                                \
        }                                                                                                              \
    }

DEFINE_CHECK_OP_IMPL(Check_EQ, ==)
DEFINE_CHECK_OP_IMPL(Check_NE, !=)
DEFINE_CHECK_OP_IMPL(Check_LE, <=)
DEFINE_CHECK_OP_IMPL(Check_LT, <)
DEFINE_CHECK_OP_IMPL(Check_GE, >=)
DEFINE_CHECK_OP_IMPL(Check_GT, >)

#define ALOG_STREAM(logger, level)                                                                                     \
    if (__builtin_expect(logger->isLevelEnabled(level), 0))                                                            \
    alog::LogStream(logger, level, __FILE__, __LINE__, __FUNCTION__, 0)

#define ALOG_STREAM_IF(logger, level, condition)                                                                       \
    if (__builtin_expect(logger->isLevelEnabled(level), 0))                                                            \
    !(condition) ? static_cast<void>(0)                                                                                \
                 : alog::LogStreamVoidify() & alog::LogStream(logger, level, __FILE__, __LINE__, __FUNCTION__, 0)

#define ALOG_STREAM_ASSERT(logger, condition)                                                                          \
    ALOG_STREAM_IF(logger, alog::LOG_LEVEL_FATAL, !(condition)) << "Assert failed: " #condition

#define LOG_EVERY_N_VARNAME(base, line) LOG_EVERY_N_VARNAME_CONCAT(base, line)
#define LOG_EVERY_N_VARNAME_CONCAT(base, line) base##line

#define LOG_OCCURRENCES LOG_EVERY_N_VARNAME(occurrences_, __LINE__)
#define LOG_OCCURRENCES_MOD_N LOG_EVERY_N_VARNAME(occurrences_mod_n_, __LINE__)

#define ALOG_STREAM_EVERY_N(logger, level, n)                                                                          \
    static thread_local int LOG_OCCURRENCES = 0, LOG_OCCURRENCES_MOD_N = 0;                                            \
    __sync_add_and_fetch(&LOG_OCCURRENCES, 1);                                                                         \
    if (__sync_add_and_fetch(&LOG_OCCURRENCES_MOD_N, 1) > n) {                                                         \
        __sync_sub_and_fetch(&LOG_OCCURRENCES_MOD_N, n);                                                               \
    }                                                                                                                  \
    if (LOG_OCCURRENCES_MOD_N == 1 && __builtin_expect(logger->isLevelEnabled(level), 0))                              \
    alog::LogStream(logger, level, __FILE__, __LINE__, __FUNCTION__, LOG_OCCURRENCES)

#define ALOG_STREAM_FIRST_N(logger, level, n)                                                                          \
    static thread_local int LOG_OCCURRENCES = 0;                                                                       \
    if (LOG_OCCURRENCES <= n) {                                                                                        \
        __sync_add_and_fetch(&LOG_OCCURRENCES, 1);                                                                     \
    }                                                                                                                  \
    if (LOG_OCCURRENCES <= n && __builtin_expect(logger->isLevelEnabled(level), 0))                                    \
    alog::LogStream(logger, level, __FILE__, __LINE__, __FUNCTION__, LOG_OCCURRENCES)

#define ALOG_STREAM_IF_EVERY_N(logger, level, condition, n)                                                            \
    static thread_local int LOG_OCCURRENCES = 0, LOG_OCCURRENCES_MOD_N = 0;                                            \
    __sync_add_and_fetch(&LOG_OCCURRENCES, 1);                                                                         \
    if (__sync_add_and_fetch(&LOG_OCCURRENCES_MOD_N, 1) > n) {                                                         \
        __sync_sub_and_fetch(&LOG_OCCURRENCES_MOD_N, n);                                                               \
    }                                                                                                                  \
    if ((condition) && LOG_OCCURRENCES_MOD_N == 1 && __builtin_expect(logger->isLevelEnabled(level), 0))               \
    alog::LogStream(logger, level, __FILE__, __LINE__, __FUNCTION__, LOG_OCCURRENCES)

#define ALOG_STREAM_EVERY_T(logger, level, seconds)                                                                    \
    constexpr std::chrono::nanoseconds LOG_TIME_PERIOD =                                                               \
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::duration<double>(seconds));                  \
    static thread_local std::atomic<int64_t> LOG_PREVIOUS_TIME_RAW;                                                    \
    const auto LOG_CURRENT_TIME =                                                                                      \
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch());     \
    const auto LOG_PREVIOUS_TIME = LOG_PREVIOUS_TIME_RAW.load(std::memory_order_relaxed);                              \
    const auto LOG_TIME_DELTA = LOG_CURRENT_TIME - std::chrono::nanoseconds(LOG_PREVIOUS_TIME);                        \
    if (LOG_TIME_DELTA > LOG_TIME_PERIOD) {                                                                            \
        LOG_PREVIOUS_TIME_RAW.store(std::chrono::duration_cast<std::chrono::nanoseconds>(LOG_CURRENT_TIME).count(),    \
                                    std::memory_order_relaxed);                                                        \
    }                                                                                                                  \
    if (LOG_TIME_DELTA > LOG_TIME_PERIOD)                                                                              \
    alog::LogStream(logger, level, __FILE__, __LINE__, __FUNCTION__, 0)

#define ALOG_CHECK(logger, level, condition)                                                                           \
    ALOG_STREAM_IF(logger, level, __builtin_expect(!(condition), 0)) << "Check failed: " #condition " "

#define ALOG_CHECK_EQ(logger, level, val1, val2) INNER_CHECK_OP(logger, level, _EQ, ==, val1, val2)
#define ALOG_CHECK_NE(logger, level, val1, val2) INNER_CHECK_OP(logger, level, _NE, !=, val1, val2)
#define ALOG_CHECK_LE(logger, level, val1, val2) INNER_CHECK_OP(logger, level, _LE, <=, val1, val2)
#define ALOG_CHECK_LT(logger, level, val1, val2) INNER_CHECK_OP(logger, level, _LT, <, val1, val2)
#define ALOG_CHECK_GE(logger, level, val1, val2) INNER_CHECK_OP(logger, level, _GE, >=, val1, val2)
#define ALOG_CHECK_GT(logger, level, val1, val2) INNER_CHECK_OP(logger, level, _GT, >, val1, val2)

#define ALOG_CHECK_NOTNULL(logger, level, val)                                                                         \
    CheckNotNull(logger, level, __FILE__, __LINE__, __FUNCTION__, (val), "'" #val "' is NULL!")

#define ALOG_CHECK_STREQ(logger, level, s1, s2) INNER_CHECK_STROP(logger, level, strcmp, ==, true, s1, s2)
#define ALOG_CHECK_STRNE(logger, level, s1, s2) INNER_CHECK_STROP(logger, level, strcmp, !=, false, s1, s2)
#define ALOG_CHECK_STRCASEEQ(logger, level, s1, s2) INNER_CHECK_STROP(logger, level, strcasecmp, ==, true, s1, s2)
#define ALOG_CHECK_STRCASENE(logger, level, s1, s2) INNER_CHECK_STROP(logger, level, strcasecmp, !=, false, s1, s2)

#define ALOG_CHECK_INDEX(logger, level, I, A) ALOG_CHECK(logger, level, I < (sizeof(A) / sizeof(A[0])))
#define ALOG_CHECK_BOUND(logger, level, B, A) ALOG_CHECK(logger, level, B <= (sizeof(A) / sizeof(A[0])))

#define ALOG_CHECK_DOUBLE_EQ(logger, level, val1, val2)                                                                \
    do {                                                                                                               \
        ALOG_CHECK_LE(logger, level, (val1), (val2) + 0.000000000000001L);                                             \
        ALOG_CHECK_GE(logger, level, (val1), (val2)-0.000000000000001L);                                               \
    } while (0)
#define ALOG_CHECK_NEAR(logger, level, val1, val2, margin)                                                             \
    do {                                                                                                               \
        ALOG_CHECK_LE(logger, level, (val1), (val2) + (margin));                                                       \
        ALOG_CHECK_GE(logger, level, (val1), (val2) - (margin));                                                       \
    } while (0)

template <typename T>
T CheckNotNull(Logger *logger, int level, const char *file, const int line, const char *func, T &&t, const char *msg) {
    if (nullptr == t && __builtin_expect(logger->isLevelEnabled(level), 0)) {
        LogStream(logger, level, file, line, func, 0) << "Check failed: " << msg;
    }
    return std::forward<T>(t);
}

} // namespace alog
#endif