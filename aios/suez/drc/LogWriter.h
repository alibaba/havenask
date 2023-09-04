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
#pragma once

#include <memory>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "suez/drc/LogRecord.h"

namespace suez {

class Sink;

class LogWriter {
public:
    enum Status {
        S_OK,
        S_FAIL,
        S_IGNORE,
    };

public:
    LogWriter(std::unique_ptr<Sink> sink,
              autil::HashFunctionBasePtr hashFunc,
              const std::vector<std::string> &hashFields);
    ~LogWriter();

public:
    Status write(const LogRecord &log);
    int64_t getCommittedCheckpoint();
    const std::vector<std::string> &getHashFields() const { return _hashFields; }

private:
    std::unique_ptr<Sink> _sink;
    autil::HashFunctionBasePtr _hashFunc;
    std::vector<std::string> _hashFields;
};

} // namespace suez
