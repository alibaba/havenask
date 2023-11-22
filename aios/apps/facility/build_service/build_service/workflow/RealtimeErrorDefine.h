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

namespace build_service { namespace workflow {

enum RealtimeErrorCode {
    ERROR_NONE,
    ERROR_BUILD_REALTIME_REACH_MAX_INDEX_SIZE,
    ERROR_REOPEN_INDEX_LACKOFMEM_ERROR,
    ERROR_BUILD_DUMP,
    ERROR_SWIFT_SEEK
};

}} // namespace build_service::workflow
