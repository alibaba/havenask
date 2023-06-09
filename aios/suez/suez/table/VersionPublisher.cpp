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
#include "suez/table/VersionPublisher.h"

#include "autil/EnvUtil.h"
#include "suez/table/LocalVersionPublisher.h"
#include "suez/table/SimpleVersionPublisher.h"

namespace suez {

VersionPublisher::~VersionPublisher() {}

std::unique_ptr<VersionPublisher> VersionPublisher::create() {
    auto publishMode = autil::EnvUtil::getEnv("version_publish_mode", "");
    if (!publishMode.empty() && publishMode == "LOCAL") {
        return std::make_unique<LocalVersionPublisher>();
    }

    return std::make_unique<SimpleVersionPublisher>();
}

} // namespace suez
