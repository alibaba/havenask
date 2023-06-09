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

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"

namespace worker_framework {

class LeaderLocator {
public:
    LeaderLocator();
    virtual ~LeaderLocator();

private:
    LeaderLocator(const LeaderLocator &);
    LeaderLocator &operator=(const LeaderLocator &);

public:
    /* virtual for mock */ virtual bool
    init(cm_basic::ZkWrapper *zk, const std::string &path, const std::string &baseName);
    /* virtual for mock */ virtual std::string getLeaderAddr();
    /* virtual for mock */ virtual std::string getLeaderHTTPAddr();

public:
    static std::string getLine(const std::string str, const uint n);

public:
    // public for test
    /* virtual for mock */ virtual std::string doGetLeaderAddr();

public:
    // for test
    void setZkWrapper(cm_basic::ZkWrapper *zk) { _zk = zk; }
    cm_basic::ZkWrapper *getZkWrapper() { return _zk; }

private:
    cm_basic::ZkWrapper *_zk;
    std::string _path;
    std::string _baseName;
};

typedef std::shared_ptr<LeaderLocator> LeaderLocatorPtr;

}; // namespace worker_framework
