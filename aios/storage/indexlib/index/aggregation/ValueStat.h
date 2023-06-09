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

#include <cstddef>

namespace indexlibv2::index {

struct ValueStat {
    size_t valueCount = 0;
    size_t valueCountAfterUnique = 0;
    size_t valueBytes = 0;
    size_t valueBytesAfterUnique = 0;
    // other stat information, such as min-max value after sort
};

} // namespace indexlibv2::index
