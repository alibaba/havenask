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
#include "suez/table/LocalVersionPublisher.h"

namespace suez {

void LocalVersionPublisher::remove(const PartitionId &pid) {}

bool LocalVersionPublisher::publish(const std::string &path, const PartitionId &pid, const TableVersion &version) {
    return true;
}

bool LocalVersionPublisher::getPublishedVersion(const std::string &path,
                                                const PartitionId &pid,
                                                IncVersion versionId,
                                                TableVersion &version) {
    return false;
}

} // namespace suez
