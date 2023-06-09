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
#include "aios/network/gig/multi_call/java/GigInstance.h"
#include "kmonitor/client/KMonitorFactory.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigInstance);

const std::string DEFAULT_ALOG_CONF = R"foo(alog.rootLogger=INFO, gigAppender
alog.max_msg_len=4096
alog.appender.gigAppender=FileAppender
alog.appender.gigAppender.fileName=gig.log
alog.appender.gigAppender.flush=true
alog.appender.gigAppender.max_file_size=1000
alog.appender.gigAppender.compress=true
alog.appender.gigAppender.layout=PatternLayout
alog.appender.gigAppender.layout.LogPattern=[%%d] [%%l] [%%t,%%F -- %%f():%%n] [%%m]
alog.appender.gigAppender.log_keep_count=100
alog.logger.arpc=WARN)foo";

bool GigInstance::alogInited = false;
bool GigInstance::kmonInited = false;

bool GigInstance::isInited() { return alogInited && kmonInited; }

bool GigInstance::initKmon(const std::string &gigKmonConf) {
    if (!kmonitor::KMonitorFactory::Init(gigKmonConf)) {
        AUTIL_LOG(ERROR, "kmonitor init failed, config content:[%s]",
                  gigKmonConf.c_str());
        return false;
    }

    kmonitor::KMonitorFactory::Start();
    kmonInited = true;
    return true;
}

bool GigInstance::initAlog(const std::string &gigAlogConf) {
    if (!alogInited) {
        std::string alogConf = gigAlogConf;
        if (alogConf.empty()) {
            alogConf = DEFAULT_ALOG_CONF;
        }
        try {
            alog::Configurator::configureLoggerFromString(alogConf.c_str());
            alogInited = true;
        } catch (...) {
            std::cerr << "configure logger use [" << alogConf << "] failed"
                      << std::endl;
            return false;
        }
    }
    return true;
}

void GigInstance::reset() {
    autil::ScopedLock lock(_mutex);
    _searchService.reset();
}

bool GigInstance::isReset() {
    autil::ScopedLock lock(_mutex);
    return _searchService.use_count() == 0;
}

SearchServicePtr GigInstance::getSearchServicePtr() {
    autil::ScopedLock lock(_mutex);
    return _searchService;
}

JavaCallback GigInstance::getJavaCallback() const { return _callback; }

} // namespace multi_call
