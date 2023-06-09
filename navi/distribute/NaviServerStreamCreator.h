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

#include "navi/common.h"
#include "navi/log/NaviLogger.h"
#include <autil/Lock.h>
#include <multi_call/stream/GigServerStreamCreator.h>

namespace navi {

class Navi;

class NaviServerStreamCreator : public multi_call::GigServerStreamCreator
{
public:
    NaviServerStreamCreator(const NaviLoggerPtr &logger, Navi *navi);
    ~NaviServerStreamCreator();
private:
    NaviServerStreamCreator(const NaviServerStreamCreator &);
    NaviServerStreamCreator &operator=(const NaviServerStreamCreator &);
public:
    std::string methodName() const override;
    multi_call::GigServerStreamPtr create() override;
public:
    void decQueryCount();
    void stop();
private:
    bool incQueryCount();
private:
    autil::ReadWriteLock _rwLock;
    Navi *_navi = nullptr;
    atomic64_t _queryCount;
private:
    DECLARE_LOGGER();
};

NAVI_TYPEDEF_PTR(NaviServerStreamCreator);

}
