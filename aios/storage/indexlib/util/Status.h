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

namespace indexlib { namespace util {

// TODO: rename, conflict with indexlib::Status
enum Status {
    OK = 0,
    UNKNOWN = 1,
    FAIL = 2,
    NO_SPACE = 3,
    INVALID_ARGUMENT = 4,
    NOT_FOUND = 101,
    DELETED = 102,
};
}} // namespace indexlib::util
