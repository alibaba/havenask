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

#include <string>

#include "autil/legacy/jsonizable.h"

namespace suez {

class SchedulerInfo : public autil::legacy::Jsonizable {
public:
    SchedulerInfo();
    ~SchedulerInfo();

public:
    bool operator==(const SchedulerInfo &info) const;
    bool operator!=(const SchedulerInfo &info) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool parseFrom(const std::string &schedulerInfoStr);

public:
    bool getCm2TopoInfo(std::string &cm2TopoInfo) const;

    static const std::string UNINITIALIZED_VALUE;

private:
    std::string _cm2TopoInfo;
};

} // namespace suez
