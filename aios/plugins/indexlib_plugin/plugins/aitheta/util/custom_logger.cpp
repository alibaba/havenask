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
#include "indexlib_plugin/plugins/aitheta/util/custom_logger.h"
#include "autil/Lock.h"
#include <sstream>

namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, CustomLogger);

int CustomLogger::init(const aitheta::LoggerParams &params) { return 0; }

//! Cleanup Logger
int CustomLogger::cleanup(void) { return 0; }

//! Log Message
void CustomLogger::log(int level, const char *file, int line, const char *format, va_list args) {
    char buffer[8192];
    std::ostringstream stream;
    vsnprintf(buffer, sizeof(buffer), format, args);
    stream << '[' << GetBaseName(file) << ':' << line << "]: " << ' ' << buffer << ' ';

    if (level == aitheta::IndexLogger::LEVEL_DEBUG) {
        IE_LOG(DEBUG, "%s", stream.str().c_str());
    } else if (level == aitheta::IndexLogger::LEVEL_INFO) {
        IE_LOG(INFO, "%s", stream.str().c_str());
    } else if (level == aitheta::IndexLogger::LEVEL_WARN) {
        IE_LOG(WARN, "%s", stream.str().c_str());
    } else if (level == aitheta::IndexLogger::LEVEL_ERROR) {
        IE_LOG(ERROR, "%s", stream.str().c_str());
    } else if (level == aitheta::IndexLogger::LEVEL_FATAL) {
        IE_LOG(FATAL, "%s", stream.str().c_str());
    } else {
        IE_LOG(INFO, "%s", stream.str().c_str());
    }
}

void RegisterGlobalIndexLogger() {
    static bool isDone = false;
    static autil::ReadWriteLock mRwLock;
    autil::ScopedReadLock lock(mRwLock);
    if (isDone == true) {
        return;
    }
    aitheta::IndexLogger::Pointer loggerPtr(new CustomLogger());
    aitheta::IndexLoggerBroker::Register(loggerPtr);
    isDone = true;
}

}
}
