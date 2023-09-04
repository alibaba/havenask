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
#include "suez/drc/DrcConfig.h"

namespace suez {

void SourceConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("type", type, type);
    json.Jsonize("parameters", parameters, parameters);
}

void SinkConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("name", name, name);
    json.Jsonize("type", type, type);
    json.Jsonize("source_start_offset", sourceStartOffset, sourceStartOffset);
    json.Jsonize("distribution_config", distConfig, distConfig);
    json.Jsonize("parameters", parameters, parameters);
}

void DrcConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("enabled", _enabled, _enabled);
    json.Jsonize("checkpoint_root", _checkpointRoot, _checkpointRoot);
    json.Jsonize("source", _sourceConfig, _sourceConfig);
    json.Jsonize("sinks", _sinkConfigs, _sinkConfigs);
    json.Jsonize("log_loop_interval_ms", _logLoopIntervalMs, _logLoopIntervalMs);
    json.Jsonize("start_offset", _startOffset, _startOffset);
    json.Jsonize("share_mode", _shareMode, _shareMode);
}

DrcConfig DrcConfig::deriveForSingleSink(const SinkConfig &sink) const {
    auto derived = *this;

    // rewrite checkpoint root
    if (derived._checkpointRoot.back() != '/') {
        derived._checkpointRoot += '/';
    }
    derived._checkpointRoot += sink.name;

    // rewrite sink config
    derived._sinkConfigs.clear();
    derived._sinkConfigs.push_back(sink);

    // rewrite offset
    if (sink.sourceStartOffset != 0) {
        derived._startOffset = sink.sourceStartOffset;
    }

    return derived;
}

} // namespace suez
