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
#include <string>

#include "autil/Lock.h"
#include "suez/table/InnerDef.h"

namespace suez {

class IndexChecker {
private:
    enum State
    {
        CANCELLED,
        RUNNING,
    };

public:
    IndexChecker();
    ~IndexChecker();

    void cancel();
    DeployStatus waitIndexReady(const std::string &checkPath);

private:
    IndexChecker(const IndexChecker &);
    IndexChecker &operator=(const IndexChecker &);

private:
    autil::ThreadMutex _mutex;
    std::shared_ptr<State> _state;
};

} // namespace suez
