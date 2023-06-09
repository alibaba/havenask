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
#ifndef __INDEXLIB_CONFIG_DEFINE_H
#define __INDEXLIB_CONFIG_DEFINE_H

namespace indexlib { namespace config {

#define IE_CONFIG_ASSERT_EQUAL(a, b, msg)                                                                              \
    do {                                                                                                               \
        if ((a) != (b)) {                                                                                              \
            AUTIL_LEGACY_THROW(util::AssertEqualException, msg);                                                       \
        }                                                                                                              \
    } while (false)

#define IE_CONFIG_ASSERT(a, msg)                                                                                       \
    do {                                                                                                               \
        if (!(a)) {                                                                                                    \
            AUTIL_LEGACY_THROW(util::AssertEqualException, msg);                                                       \
        }                                                                                                              \
    } while (false)

#define IE_CONFIG_ASSERT_PARAMETER_VALID(a, msg)                                                                       \
    do {                                                                                                               \
        if (!(a)) {                                                                                                    \
            AUTIL_LEGACY_THROW(util::BadParameterException, msg);                                                      \
        }                                                                                                              \
    } while (false)
}} // namespace indexlib::config

#endif //__INDEXLIB_CONFIG_DEFINE_H
