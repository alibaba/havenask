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
#ifndef ISEARCH_BS_HEARTBEAT_DEFINE_H
#define ISEARCH_BS_HEARTBEAT_DEFINE_H

#include <cstdint>

namespace build_service { namespace proto {

static const int64_t ADMIN_HEARTBEAT_INTERVAL = 1 * 1000 * 1000;
static const int64_t WORKER_EXEC_HEARTBEAT_INTERVAL = 1 * 1000 * 1000;

}} // namespace build_service::proto

#endif // ISEARCH_BS_HEARTBEAT_DEFINE_H
