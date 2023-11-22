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
#include "access_log/local/LocalAccessLogAccessor.h"

#include "access_log/local/LocalAccessLogReader.h"
#include "access_log/local/LocalAccessLogWriter.h"
#include "autil/Log.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, LocalAccessLogAccessor);

namespace access_log {

LocalAccessLogAccessor::LocalAccessLogAccessor(const string &logName, const string &logDir)
    : AccessLogAccessor(logName), _logDir(logDir) {}

LocalAccessLogAccessor::~LocalAccessLogAccessor() {}

AccessLogWriter *LocalAccessLogAccessor::createWriter() {
    LocalAccessLogWriter *writer = new LocalAccessLogWriter(_logName);
    return writer;
}

AccessLogReader *LocalAccessLogAccessor::createReader() {
    LocalAccessLogReader *reader = new LocalAccessLogReader(_logName, _logDir);
    if (!reader->init()) {
        delete reader;
        return nullptr;
    }
    return reader;
}

} // namespace access_log
