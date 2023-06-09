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
#include "suez/drc/LogReader.h"

#include "suez/drc/Source.h"

namespace suez {

LogReader::LogReader(std::unique_ptr<Source> source) : _source(std::move(source)) {}

LogReader::~LogReader() {}

bool LogReader::seek(int64_t offset) { return _source->seek(offset); }

bool LogReader::read(LogRecord &record) {
    std::string data;
    int64_t logId;
    if (!_source->read(data, logId)) {
        return false;
    }
    return record.init(logId, std::move(data));
}

int64_t LogReader::getLatestLogId() const { return _source->getLatestLogId(); }

} // namespace suez
