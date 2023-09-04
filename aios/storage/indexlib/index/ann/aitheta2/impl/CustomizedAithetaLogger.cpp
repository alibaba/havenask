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
#include "indexlib/index/ann/aitheta2/impl/CustomizedAithetaLogger.h"

#include <ailego/io/file.h>
#include <ailego/utility/time_helper.h>
#include <sstream>
#include <thread>

#include "autil/Lock.h"

namespace indexlibv2::index::ann {

AUTIL_LOG_SETUP(indexlib.index, CustomizedAiThetaLogger);

int CustomizedAiThetaLogger::init(const aitheta2::IndexParams& params) { return 0; }

int CustomizedAiThetaLogger::cleanup(void) { return 0; }

void CustomizedAiThetaLogger::log(int level, const char* file, int line, const char* format, va_list args)
{
    thread_local std::unique_ptr<char[]> buffer(new char[BUFFER_SIZE]);
    std::ostringstream stream;
    stream << ailego::File::BaseName(file) << ':' << line << " ";
    vsnprintf(buffer.get(), BUFFER_SIZE, format, args);
    stream << buffer.get();
    if (level == aitheta2::IndexLogger::LEVEL_DEBUG) {
        AUTIL_LOG(DEBUG, "%s", stream.str().c_str());
    } else if (level == aitheta2::IndexLogger::LEVEL_INFO) {
        AUTIL_LOG(INFO, "%s", stream.str().c_str());
    } else if (level == aitheta2::IndexLogger::LEVEL_WARN) {
        AUTIL_LOG(WARN, "%s", stream.str().c_str());
    } else if (level == aitheta2::IndexLogger::LEVEL_ERROR) {
        AUTIL_LOG(ERROR, "%s", stream.str().c_str());
    } else if (level == aitheta2::IndexLogger::LEVEL_FATAL) {
        AUTIL_LOG(FATAL, "%s", stream.str().c_str());
    } else {
        AUTIL_LOG(INFO, "%s", stream.str().c_str());
    }
}

void CustomizedAiThetaLogger::RegisterIndexLogger()
{
    static bool isRegistered = false;
    static autil::ReadWriteLock mRwLock;
    autil::ScopedReadLock lock(mRwLock);
    if (isRegistered == true) {
        return;
    }
    aitheta2::IndexLogger::Pointer loggerPtr(new CustomizedAiThetaLogger());
    aitheta2::IndexLoggerBroker::Register(loggerPtr);
    isRegistered = true;
}

} // namespace indexlibv2::index::ann
