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
#include "build_service/builder/BuilderV2.h"

namespace build_service::builder {

BuilderV2::BuilderV2(const proto::BuildId& buildId) : _buildId(buildId) {}

BuilderV2::~BuilderV2() {}

const proto::BuildId& BuilderV2::getBuildId() const { return _buildId; }

} // namespace build_service::builder
