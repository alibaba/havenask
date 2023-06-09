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

#include <atomic>
#include <cstdint>

#include "suez/table/TableVersion.h"

namespace suez {

class TableBuilder {
public:
    TableBuilder();
    virtual ~TableBuilder();

public:
    virtual bool start() = 0;
    virtual void stop() = 0;
    virtual void suspend() = 0;
    virtual void resume() = 0;
    virtual void skip(int64_t timestamp) = 0;
    virtual void forceRecover() = 0;
    virtual bool isRecovered() = 0;
    virtual bool needCommit() const = 0;
    virtual std::pair<bool, TableVersion> commit() = 0;

    void setAllowCommit(bool flag);

protected:
    std::atomic<bool> _allowCommit{true};
};

} // namespace suez
