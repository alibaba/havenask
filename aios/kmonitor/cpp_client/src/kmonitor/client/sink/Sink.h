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

#include "kmonitor/client/core/MetricsRecord.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class Sink {
public:
    Sink(const std::string &name) : name_(name) {}
    virtual ~Sink() {}
    virtual bool Init() = 0;
    virtual void PutMetrics(MetricsRecord *record) = 0;
    virtual void Flush() = 0;

public:
    const std::string GetName() const { return name_; }

private:
    std::string name_;
};

TYPEDEF_PTR(Sink);

END_KMONITOR_NAMESPACE(kmonitor);
