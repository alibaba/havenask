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
#include "suez/worker/ManuallySink.h"

#include "suez/worker/KMonitorManager.h"

using namespace kmonitor;
using namespace std;
using namespace autil;

namespace suez {

void MetricsRecordRep::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("name", name, name);
    json.Jsonize("tagstring", tagstring, tagstring);
    json.Jsonize("timestamp", timestamp, timestamp);
    json.Jsonize("values", values, values);
}

ManuallySink::ManuallySink() : Sink("ManuallySink") {}
ManuallySink::~ManuallySink() {}

void ManuallySink::PutMetrics(MetricsRecord *record) {
    static const std::string delim = "_";
    MetricsRecordRep rep;
    for (const auto &value : record->Values()) {
        rep.name = value->Name();
        rep.tagstring = record->Tags()->ToString();
        rep.timestamp = record->Timestamp();
        StringUtil::fromString(value->Value(), rep.values, delim);
        _records.emplace_back(std::move(rep));
    }
}

void ManuallySink::Flush() {
    auto records = std::move(_records);
    _lastResult = FastToJsonString(records);
}

const std::string &ManuallySink::getLastResult() const { return _lastResult; }

} // namespace suez
