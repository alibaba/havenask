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

namespace worker_framework {

class WorkerState {
public:
    enum ErrorCode {
        EC_OK,
        EC_FAIL,
        EC_NOT_EXIST, // for read
        EC_UPDATE     // state has been updated
    };

public:
    WorkerState() {}
    virtual ~WorkerState() {}

private:
    WorkerState(const WorkerState &);
    WorkerState &operator=(const WorkerState &);

public:
    virtual ErrorCode write(const std::string &content) = 0;
    virtual ErrorCode read(std::string &content) = 0;
};

typedef std::shared_ptr<WorkerState> WorkerStatePtr;

}; // namespace worker_framework
