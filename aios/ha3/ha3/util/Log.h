#ifndef ISEARCH_LOG_H_
#define ISEARCH_LOG_H_

#include <alog/Logger.h>
#include <alog/Configurator.h>
#include <iostream>
#include <navi/common.h>
#include <navi/log/NaviLogger.h>

#define HA3_LOG_SETUP(n, c)  alog::Logger *c::_logger   \
    = alog::Logger::getLogger("ha3."#n "." #c)

#define HA3_LOG_SETUP_TEMPLATE(n, c, T) template <typename T>   \
    alog::Logger *c<T>::_logger                                 \
    = alog::Logger::getLogger("ha3."#n "." #c)

#define HA3_LOG_SETUP_TEMPLATE_2(n,c, T1, T2) template <typename T1, typename T2> \
    alog::Logger *c<T1, T2>::_logger                           \
    = alog::Logger::getLogger("ha3." #n "."  #c)

#define HA3_LOG_SETUP_TEMPLATE_3(n,c, T1, T2, T3)              \
    template <typename T1, typename T2, typename T3>           \
    alog::Logger *c<T1, T2, T3>::_logger                       \
    = alog::Logger::getLogger("ha3." #n "."  #c)

#define HA3_ROOT_LOG_CONFIG() alog::Configurator::configureRootLogger()
#define HA3_LOG_CONFIG(filename) do {                           \
        try {                                                   \
            alog::Configurator::configureLogger(filename);      \
        } catch(std::exception &e) {                            \
            std::cerr << "WARN! Failed to configure logger!"    \
                      << e.what() << ",use default log conf."   \
                      << std::endl;                             \
            HA3_ROOT_LOG_CONFIG();                              \
        }                                                       \
    }while(0)

#define HA3_LOG_CONFIG_FROM_STRING(content) do {                        \
        try {                                                           \
            alog::Configurator::configureLoggerFromString(content);     \
        } catch(std::exception &e) {                                    \
            std::cerr << "WARN! Failed to configure logger!"            \
                      << e.what() << ",use default log conf."           \
                      << std::endl;                                     \
            HA3_ROOT_LOG_CONFIG();                                      \
        }                                                               \
    }while(0)

#define HA3_ROOT_LOG_SETLEVEL(level)                                  \
    alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_##level)

#define HA3_LOG_DECLARE() static alog::Logger *_logger

#define HA3_ENSURE_STRING_LITERAL(x)  do { (void) x ""; } while(0)

#ifndef NDEBUG
#define HA3_LOG_INTERNAL(level, format, args...)                        \
    do {                                                                \
        HA3_ENSURE_STRING_LITERAL(format);                              \
        ALOG_LOG(_logger, alog::LOG_LEVEL##level, format, ##args);      \
    } while (0)
#else
#define HA3_LOG_INTERNAL(level, format, args...) ALOG_LOG(_logger, alog::LOG_LEVEL##level, format, ##args)
#endif

#define HA3_LOG(level, format, args...) HA3_LOG_INTERNAL(_##level, format, ##args)

#define HA3_DECLARE_AND_SETUP_LOGGER(c) static alog::Logger *_logger    \
    = alog::Logger::getLogger("ha3." #c)

#define HA3_LOG_SHUTDOWN() alog::Logger::shutdown()

#define HA3_LOG_FLUSH() alog::Logger::flushAll()


#define SQL_LOG(level, format, args...)                                 \
    {                                                                   \
        if (navi::NAVI_TLS_LOGGER ) {                                   \
            NAVI_KERNEL_LOG_INTERNAL(_##level, format, ##args);         \
        } else {                                                        \
            HA3_LOG_INTERNAL(_##level, format, ##args);                 \
        }                                                               \
    }



#endif
