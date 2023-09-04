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
#ifndef GLOBAL_FLAGS_H
#define GLOBAL_FLAGS_H

#include <cstdint>
#include <sstream>

namespace anet {
namespace flags {

#define DECLARE_GLOBAL_FLAG(type, name)                                                                                \
    type get##name(void);                                                                                              \
    void set##name(type newval);

DECLARE_GLOBAL_FLAG(int, ConnectTimeout);
DECLARE_GLOBAL_FLAG(bool, ChecksumState);
DECLARE_GLOBAL_FLAG(int64_t, MaxConnectionCount);
DECLARE_GLOBAL_FLAG(bool, EnableSocketNonBlockConnect);

/* function to dump all configuration. */
int dump(std::ostringstream &buf);

} // namespace flags
} // namespace anet
#endif
