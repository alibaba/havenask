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

#include <functional>
#include <stdint.h>
#include <string>

#include "autil/Thread.h"
#include "build_service/util/Log.h"

namespace build_service { namespace workflow {

class AsyncStarter
{
public:
    AsyncStarter();
    ~AsyncStarter();

private:
    AsyncStarter(const AsyncStarter&);
    AsyncStarter& operator=(const AsyncStarter&);

public:
    typedef std::function<bool()> func_type;

public:
    void setName(const std::string& name) { _name = name; }
    const std::string& getName() { return _name; }

    void asyncStart(const func_type& func);
    void stop();

private:
    void loopStart();
    void getMaxRetryIntervalTime();

private:
    volatile bool _running;
    func_type _func;
    autil::ThreadPtr _startThread;
    int64_t _maxRetryIntervalTime;
    std::string _name;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::workflow
