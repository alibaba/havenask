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

#include "autil/Lock.h"
#include "navi/common.h"

namespace navi {

class NaviClosure {
public:
    NaviClosure() = default;
    virtual ~NaviClosure() = default;
    NaviClosure(const NaviClosure &) = delete;
    NaviClosure &operator=(const NaviClosure &) = delete;

public:
    virtual void run() = 0;
};

class WaitClosure {
public:
    WaitClosure();
    virtual ~WaitClosure();

private:
    WaitClosure(const WaitClosure &);
    WaitClosure &operator=(const WaitClosure &);

public:
    void wait();
    void notify();
    bool isFinish() const;

protected:
    volatile bool _isFinished;
    autil::ThreadCond _cond;
};

}
