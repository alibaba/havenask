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

#include <sstream>

#include "autil/legacy/jsonizable.h"
#include "kmonitor/client/sink/Sink.h"

namespace kmonitor {
class MetricsRecord;
} // namespace kmonitor

namespace suez {

class KMonitorManager;

class MetricsRecordRep : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

public:
    std::string name;
    std::string tagstring;
    int64_t timestamp;
    std::vector<double> values;
};

class ManuallySink : public kmonitor::Sink {
public:
    ManuallySink();
    ~ManuallySink() override;

public:
    bool Init() override { return true; }
    void PutMetrics(kmonitor::MetricsRecord *record) override;
    void Flush() override;

public:
    const std::string &getLastResult() const;

private:
    std::string _lastResult;
    std::vector<MetricsRecordRep> _records;
};

} // namespace suez
