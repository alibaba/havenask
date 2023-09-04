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

#include <stdint.h>
#include <string>

namespace suez {

class TimeRecorder {
public:
    TimeRecorder();
    ~TimeRecorder();

private:
    TimeRecorder(const TimeRecorder &);
    TimeRecorder &operator=(const TimeRecorder &);

public:
    void record(const std::string &msg);
    void recordAndReset(const std::string &msg);
    void reset();

private:
    int64_t _beginTimeStamp;
};

} // namespace suez
