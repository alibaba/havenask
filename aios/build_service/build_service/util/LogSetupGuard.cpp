#include "build_service/util/LogSetupGuard.h"
#include "build_service/util/Log.h"

using namespace std;

namespace build_service {
namespace util {

const std::string LogSetupGuard::CONSOLE_LOG_CONF = "\n\
alog.rootLogger=INFO, bsAppender\n                                      \
alog.max_msg_len=4096\n                                                 \
alog.appender.bsAppender=ConsoleAppender\n                              \
alog.appender.bsAppender.fileName=bs.log\n                              \
alog.appender.bsAppender.layout=PatternLayout\n                         \
alog.appender.bsAppender.layout.LogPattern=[%%d] [%%l] [%%t,%%F -- %%f():%%n] [%%m]\n \
\n                                                                      \
alog.logger.BS.builder.Builder=TRACE1\n                                 \
\n                                                                      \
alog.logger.BS.common.PkTracer=TRACE1, PkTraceAppender\n              \
inherit.BS.common.PkTracer=false\n                                    \
alog.appender.PkTraceAppender=ConsoleAppender\n                          \
alog.appender.PkTraceAppender.fileName=pk_trace.log\n                 \
alog.appender.PkTraceAppender.flush=true\n                            \
alog.appender.PkTraceAppender.max_file_size=100\n                     \
alog.appender.PkTraceAppender.layout=PatternLayout\n                  \
alog.appender.PkTraceAppender.layout.LogPattern=%%m %%d\n             \
alog.appender.PkTraceAppender.compress=true\n                         \
alog.appender.PkTraceAppender.log_keep_count=100\n                    \
";

const std::string LogSetupGuard::FILE_LOG_CONF = "\n\
alog.rootLogger=INFO, bsAppender\n                  \
alog.max_msg_len=4096\n                             \
alog.appender.bsAppender=FileAppender\n             \
alog.appender.bsAppender.fileName=bs.log\n                              \
alog.appender.bsAppender.layout=PatternLayout\n                         \
alog.appender.bsAppender.layout.LogPattern=[%%d] [%%l] [%%t,%%F -- %%f():%%n] [%%m]\n \
\n                                                                      \
alog.logger.BS.builder.Builder=TRACE1\n                                 \
\n                                                                      \
alog.logger.BS.common.PkTracer=TRACE1, PkTraceAppender\n              \
inherit.BS.common.PkTracer=false\n                                    \
alog.appender.PkTraceAppender=FileAppender\n                          \
alog.appender.PkTraceAppender.fileName=pk_trace.log\n                 \
alog.appender.PkTraceAppender.flush=true\n                            \
alog.appender.PkTraceAppender.max_file_size=100\n                     \
alog.appender.PkTraceAppender.layout=PatternLayout\n                  \
alog.appender.PkTraceAppender.layout.LogPattern=%%m %%d\n             \
alog.appender.PkTraceAppender.compress=true\n                         \
alog.appender.PkTraceAppender.log_keep_count=100\n        \
alog.logger.ErrorLogCollector=TRACE1,ErrorLogCollectorAppender\n        \
inherit.ErrorLogCollector=false\n                                       \
alog.appender.ErrorLogCollectorAppender=FileAppender\n                  \
alog.appender.ErrorLogCollectorAppender.fileName=error_log_collector.log\n \
alog.appender.ErrorLogCollectorAppender.flush=true\n                    \
alog.appender.ErrorLogCollectorAppender.max_file_size=100\n             \
alog.appender.ErrorLogCollectorAppender.layout=PatternLayout\n          \
alog.appender.ErrorLogCollectorAppender.layout.LogPattern=[%%d] [%%l] [%%t,%%F -- %%f():%%n] [%%m]\n \
alog.appender.ErrorLogCollectorAppender.compress=true\n                 \
alog.appender.ErrorLogCollectorAppender.log_keep_count=100\n            \
";

LogSetupGuard::LogSetupGuard(const string &logConf) {
    BS_LOG_CONFIG_FROM_STRING(logConf.c_str());
}

LogSetupGuard::~LogSetupGuard() {
    //dont shutdown alog, may trigger coredump
    //BS_LOG_SHUTDOWN();
    BS_LOG_FLUSH();
}

}
}
