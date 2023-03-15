#pragma once

#include "alog/Logger.h"
#include "autil/Log.h"

#define BASIC_ROOT_LOG_CONFIG() alog::Configurator::configureRootLogger()
#define PLUG_LOG_CONFIG(filename) do {                         \
        try {                                                   \
            alog::Configurator::configureLogger(filename);      \
        } catch(std::exception &e) {                            \
            std::cerr << "WARN! Failed to configure logger!"    \
                      << e.what() << ",use default log conf."   \
                      << std::endl;                             \
            BASIC_ROOT_LOG_CONFIG();                            \
        }                                                       \
    }while(0)

#define BASIC_ROOT_LOG_SETLEVEL(level)                                  \
    alog::Logger::getRootLogger()->setLevel(alog::LOG_LEVEL_##level)

#define PLUG_LOG_DECLARE() static alog::Logger *_logger

#define PLUG_LOG_SETUP(n, c) alog::Logger *c::_logger  \
    = alog::Logger::getLogger("plugin." #n "." #c)

#define PLUG_LOG_SETUP_TEMPLATE(n, c, T) template <typename T> \
    alog::Logger *c<T>::_logger                                 \
    = alog::Logger::getLogger("plugin." #n "." #c)

#define PLUG_LOG_SETUP_TEMPLATE_2(n,c, T1, T2) template <typename T1, typename T2> \
    alog::Logger *c<T1, T2>::_logger                           \
    = alog::Logger::getLogger("plugin." #n "."  #c)

#define PLUG_LOG(level, format, args...) ALOG_LOG(_logger, alog::LOG_LEVEL_##level, format, ##args)

#define BASIC_DECLARE_AND_SETUP_LOGGER(n, c) static alog::Logger *_logger \
    = alog::Logger::getLogger("plugin." #n "." #c)
#define PLUG_LOG_SHUTDOWN() alog::Logger::shutdown()
#define PLUG_LOG_FLUSH() alog::Logger::flushAll()
