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
#include "aios/network/gig/multi_call/metric/WorkerMetricReporter.h"

using namespace std;
using namespace kmonitor;
namespace multi_call {
AUTIL_LOG_SETUP(multi_call, WorkerMetricReporter);

WorkerMetricReporter::WorkerMetricReporter(KMonitor *kMonitor,
                                           const MetaEnv &metaEnv)
    : _kMonitor(kMonitor) {
    if (_kMonitor) {
        std::map<std::string, std::string> tagkv;
        _defaultTags = _kMonitor->GetMetricsTags(tagkv); // empty tagkv
    }
    DEFINE_METRIC(kMonitor, QueueSize, "buildin.queueSize", GAUGE, NORMAL,
                  _defaultTags);
    DEFINE_METRIC(kMonitor, SubUpdateSuccQps, "buildin.subUpdateSuccQps", QPS,
                  NORMAL, _defaultTags);
    DEFINE_METRIC(kMonitor, SubUpdateFailedQps, "buildin.subUpdateFailedQps",
                  QPS, NORMAL, _defaultTags);
    DEFINE_METRIC(kMonitor, CreateSnapshotSuccQps,
                  "buildin.createSnapshotSuccQps", QPS, NORMAL, _defaultTags);
    DEFINE_METRIC(kMonitor, CreateSnapshotFailQps,
                  "buildin.createSnapshotFailQps", QPS, NORMAL, _defaultTags);
    DEFINE_METRIC(kMonitor, UpdateSnapshotQps, "buildin.updateSnapshotQps", QPS,
                  NORMAL, _defaultTags);
}

WorkerMetricReporter::~WorkerMetricReporter() {}

} // namespace multi_call
