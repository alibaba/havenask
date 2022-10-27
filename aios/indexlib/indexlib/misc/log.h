#ifndef __INDEXLIB_LOG_H
#define __INDEXLIB_LOG_H

#include <alog/Logger.h>
#include <alog/Configurator.h>
#include <iostream>
#include "indexlib/misc/error_log_collector.h"

#define IE_LOGGER_NAMESPACE alog

#define IE_ROOT_LOG_CONFIG() alog::Configurator::configureRootLogger()

#define IE_LOG_CONFIG(filename) do {                            \
        try {                                                   \
            alog::Configurator::configureLogger(filename);      \
        } catch(std::exception &e) {                            \
            std::cerr << "WARN! Failed to configure logger!"    \
                      << e.what() << ",use default log conf."   \
                      << std::endl;                             \
            IE_ROOT_LOG_CONFIG();                               \
        }                                                       \
    }while(0)

#define IE_ROOT_LOG_SETLEVEL(level) \
    alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_##level)
#define IE_LOG_DECLARE() static alog::Logger *_logger
#define IE_LOG_SETUP(n,c) alog::Logger *c::_logger \
    = alog::Logger::getLogger("indexlib." #n "."  #c)
#define IE_LOG_TEST() \
    do {\
    std::cout << "*** in log test of alog ***" << endl;     \
    }while(0)

#define IE_LOG_SETUP_TEMPLATE(n,c) template <typename T>        \
    alog::Logger *c<T>::_logger                                 \
    = alog::Logger::getLogger("indexlib." #n "."  #c)

#define IE_LOG_SETUP_TEMPLATE2(n,c) template <typename T1, typename T2>     \
    alog::Logger *c<T1, T2>::_logger                                        \
    = alog::Logger::getLogger("indexlib." #n "."  #c)

#define IE_LOG_SETUP_TEMPLATE3(n,c) template <typename T1, typename T2, typename T3> \
    alog::Logger *c<T1, T2, T3>::_logger                                \
    = alog::Logger::getLogger("indexlib." #n "."  #c)

#define IE_LOG_SETUP_TEMPLATE_CUSTOM(TT1, n, c)                         \
    template <TT1 T1>                                                   \
    alog::Logger *c<T1>::_logger                                        \
    = alog::Logger::getLogger("indexlib." #n "."  #c)

#define IE_LOG_SETUP_TEMPLATE2_CUSTOM(TT1, TT2, n, c)                   \
    template <TT1 T1, TT2 T2>                                           \
    alog::Logger *c<T1, T2>::_logger                                    \
    = alog::Logger::getLogger("indexlib." #n "."  #c)

#define IE_LOG_SETUP_TEMPLATE3_CUSTOM(TT1, TT2, TT3, n, c)              \
    template <TT1 T1, TT2 T2, TT3 T3>                                   \
    alog::Logger *c<T1, T2, T3>::_logger                                \
    = alog::Logger::getLogger("indexlib." #n "."  #c)

#define IE_LOG_SETUP_TEMPLATE4_CUSTOM(TT1, TT2, TT3, TT4, n, c)         \
    template <TT1 T1, TT2 T2, TT3 T3, TT4 T4>                           \
    alog::Logger *c<T1, T2, T3, T4>::_logger                            \
    = alog::Logger::getLogger("indexlib." #n "."  #c)

#define IE_LOG_SETUP_TEMPLATE5_CUSTOM(TT1, TT2, TT3, TT4, TT5, n, c)    \
    template <TT1 T1, TT2 T2, TT3 T3, TT4 T4, TT5 T5>                   \
    alog::Logger *c<T1, T2, T3, T4, T5>::_logger                        \
    = alog::Logger::getLogger("indexlib." #n "."  #c)

#define IE_LOG_SETUP_TEMPLATE6_CUSTOM(TT1, TT2, TT3, TT4, TT5, TT6, n, c) \
    template <TT1 T1, TT2 T2, TT3 T3, TT4 T4, TT5 T5, TT6 T6>           \
    alog::Logger *c<T1, T2, T3, T4, T5, T6>::_logger                    \
    = alog::Logger::getLogger("indexlib." #n "."  #c)

#define IE_LOG(level, format, args...) ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args)

#define IE_PREFIX_LOG(level, format, args...) IE_LOG(level, "[%s] " format, IE_LOG_PREFIX, ##args)

#define __INTERNAL_IE_LOG(level, format, args...) ALOG_LOG(_logger, level, format, ##args)


#define IE_INTERVAL_LOG(logInterval, level, format, args...)    \
    do {                                                        \
        static int logCounter;                                  \
        if (0 == logCounter) {                                  \
            IE_LOG(level, format, ##args);                      \
            logCounter = logInterval;                           \
        }                                                       \
        logCounter--;                                           \
    } while (0)

#define IE_INTERVAL_LOG2(logInterval, level, format, args...)   \
    do {                                                        \
        static int64_t logTimestamp;                            \
        int64_t now = TimeUtility::currentTimeInSeconds();      \
        if (now - logTimestamp > logInterval) {                 \
            IE_LOG(level, format, ##args);                      \
            logTimestamp = now;                                 \
        }                                                       \
    } while (0)


#define IE_DECLARE_AND_SETUP_LOGGER(n) static alog::Logger *_logger \
    = alog::Logger::getLogger("indexlib." #n)
#define IE_LOG_SHUTDOWN() alog::Logger::shutdown()
#define IE_LOG_FLUSH() alog::Logger::flushAll()

namespace heavenask { namespace indexlib {
class ScopedLogLevel
{
public:
    ScopedLogLevel(uint32_t logLevel, const std::string& loggerName = "indexlib")
        : mLoggerName(loggerName)
    {
        mOriginalLevel = alog::Logger::getLogger(mLoggerName.c_str())->getLevel();
        alog::Logger::getLogger(mLoggerName.c_str())->setLevel(logLevel);
    }
    ~ScopedLogLevel()
    {
        alog::Logger::getLogger(mLoggerName.c_str())->setLevel(mOriginalLevel);
    }
private:
    uint32_t mOriginalLevel;
    std::string mLoggerName;
};
}}

#define SCOPED_LOG_LEVEL(logLevel, loggerName...)                \
    ScopedLogLevel INDEXLIB_CONCATE(__scopedLogLevel_, __LINE__)(alog::LOG_LEVEL_##logLevel, ##loggerName)

#endif
