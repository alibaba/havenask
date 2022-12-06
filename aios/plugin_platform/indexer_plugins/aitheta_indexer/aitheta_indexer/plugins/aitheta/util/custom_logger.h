#ifndef __AITHETA_PLUGIN_CUSTOM_LOGGER_H__
#define __AITHETA_PLUGIN_CUSTOM_LOGGER_H__
#include <aitheta/index_logger.h>
#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>

IE_NAMESPACE_BEGIN(aitheta_plugin);

class CustomLogger : public aitheta::IndexLogger {
 public:
    CustomLogger() {}
    ~CustomLogger() {}

 public:
    int init(const aitheta::LoggerParams &params) override;

    //! Cleanup Logger
    int cleanup(void) override;

    //! Log Message
    void log(int level, const char *file, int line, const char *format, va_list args) override;

 private:
    IE_LOG_DECLARE();
};

void RegisterGlobalIndexLogger();

IE_NAMESPACE_END(aitheta_plugin);

#endif  // __AITHETA_PLUGIN_CUSTOM_LOGGER_H__
