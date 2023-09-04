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
#include "suez/drc/Checkpoint.h"

#include <cassert>

#include "autil/Log.h"
#include "suez/sdk/IpUtil.h"
#include "suez/sdk/PathDefine.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, Checkpoint);

Checkpoint::Checkpoint() { _generator = IpUtil::getHostAddress(); }

Checkpoint::~Checkpoint() {}

void Checkpoint::updatePersistedLogId(int64_t id) {
    if (id > _persistedLogId) {
        AUTIL_LOG(DEBUG, "update checkpoint from %ld to %ld", _persistedLogId, id);
        _persistedLogId = id;
    } else if (id < _persistedLogId) {
        if (id > 0) {
            // ignore id = -1
            AUTIL_LOG(ERROR, "new persisted id: %ld, last sent id: %ld", id, _persistedLogId);
        }
    }
}

void Checkpoint::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("persisted_log_id", _persistedLogId);
    json.Jsonize("generator", _generator, _generator);
}

std::string Checkpoint::toString() const { return FastToJsonString(*this); }

bool Checkpoint::fromString(const std::string &content) {
    try {
        FastFromJsonString(*this, content);
    } catch (const std::exception &e) {
        AUTIL_LOG(ERROR, "deserialize from %s failed, error: %s", content.c_str(), e.what());
        return false;
    }
    return true;
}

std::string Checkpoint::getCheckpointFileName(const std::string &root) {
    static const std::string NAME = "drc_checkpoint";
    return PathDefine::join(root, NAME);
}

} // namespace suez
