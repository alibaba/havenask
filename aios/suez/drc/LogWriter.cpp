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
#include "suez/drc/LogWriter.h"

#include "suez/drc/Sink.h"

namespace suez {

LogWriter::LogWriter(std::unique_ptr<Sink> sink,
                     autil::HashFunctionBasePtr hashFunc,
                     const std::vector<std::string> &hashFields)
    : _sink(std::move(sink)), _hashFunc(std::move(hashFunc)), _hashFields(hashFields) {}

LogWriter::~LogWriter() {}

LogWriter::Status LogWriter::write(const LogRecord &log) {
    uint32_t hashId = 0;
    if (_hashFields.size() == 1) {
        autil::StringView value;
        if (!log.getField(_hashFields[0], value)) {
            // TODO: maybe ignore this record ?
            return LogWriter::S_IGNORE;
        }
        hashId = _hashFunc->getHashId(value.data(), value.size());
    } else {
        std::vector<std::string> values;
        for (const auto &fieldName : _hashFields) {
            autil::StringView value;
            if (!log.getField(fieldName, value)) {
                return LogWriter::S_IGNORE;
            }
            values.emplace_back(value.to_string());
        }
        hashId = _hashFunc->getHashId(values);
    }
    return _sink->write(hashId, log.getData(), log.getLogId()) ? LogWriter::S_OK : LogWriter::S_FAIL;
}

int64_t LogWriter::getCommittedCheckpoint() { return _sink->getCommittedCheckpoint(); }

} // namespace suez
