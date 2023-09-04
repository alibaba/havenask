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
#include "swift/tools/LogSetup.h"

#include <iosfwd>

#include "autil/Log.h"

using namespace std;

namespace swift {
namespace tools {

const std::string LogSetup::DEFAULT_TOOLS_LOG_CONF = "\n\
alog.rootLogger=INFO, swiftAppender\n                        \
alog.max_msg_len=4096\n                                      \
alog.appender.swiftAppender=FileAppender\n                   \
alog.appender.swiftAppender.fileName=swift_admin_starter.log\n       \
alog.appender.swiftAppender.async_flush=true\n                       \
alog.appender.swiftAppender.max_file_size=100\n                      \
alog.appender.swiftAppender.layout=PatternLayout\n                   \
alog.appender.swiftAppender.layout.LogPattern=[%%d] [%%l] [%%t,%%F -- %%f():%%n] [%%m]\n \
alog.appender.swiftAppender.log_keep_count=100";

LogSetup::LogSetup(const string &logConf) { AUTIL_LOG_CONFIG_FROM_STRING(logConf.c_str()); }

LogSetup::~LogSetup() { AUTIL_LOG_SHUTDOWN(); }

}; // namespace tools
}; // namespace swift
