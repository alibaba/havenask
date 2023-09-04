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
#include <fstream>
#include <iostream>
#include <string>

#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"
#include "trace_server/TraceServerWorker.h"

using namespace std;
using namespace autil;
int main(int argc, char** argv)
{
    absl::SetProgramUsageMessage(
        "Usage: swift_doc_tracer --swiftRoot=swiftRoot --topicName=topicName --range=0-65535 --needFieldFilter=false "
        "--beginTimestamp=xxxx --endTimestamp=xxxx --docCount=100000 [--schemaPath=schemaPath]");
    absl::ParseCommandLine(argc, argv);
    std::string DEFAULT_ALOG_CONF = R"foo(alog.rootLogger=INFO, bsAppender
alog.max_msg_len=4096
alog.appender.bsAppender=FileAppender
alog.appender.bsAppender.fileName=doc_trace.log
alog.appender.bsAppender.flush=true
alog.appender.bsAppender.max_file_size=1000
alog.appender.bsAppender.compress=true
alog.appender.bsAppender.layout=PatternLayout
alog.appender.bsAppender.layout.LogPattern=[%%d] [%%l] [%%t,%%F -- %%f():%%n] [%%m]
alog.appender.bsAppender.log_keep_count=100
alog.logger.arpc=WARN)foo";
    try {
        alog::Configurator::configureLoggerFromString(DEFAULT_ALOG_CONF.c_str());
    } catch (...) {
        std::cerr << "configure logger failed" << std::endl;
        return 0;
    }

    auto worker = std::make_shared<build_service::tools::TraceServerWorker>();
    worker->init();
    worker->run();
    worker.reset();
    AUTIL_LOG_FLUSH();
    fslib::fs::FileSystem::close();
    return 0;
}
