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

#include "autil/DataBuffer.h" // IWYU pragma: keep

#define HA3_DATABUFFER_READ(member) dataBuffer.read(member);
#define HA3_DATABUFFER_WRITE(member) dataBuffer.write(member);

#define HA3_SERIALIZE_DECLARE(...)                                                                 \
    void serialize(autil::DataBuffer &dataBuffer) const __VA_ARGS__;                               \
    void deserialize(autil::DataBuffer &dataBuffer) __VA_ARGS__;

#define HA3_SERIALIZE_IMPL(ClassName, MEMBERS_MACRO)                                               \
    void ClassName::serialize(autil::DataBuffer &dataBuffer) const {                               \
        MEMBERS_MACRO(HA3_DATABUFFER_WRITE);                                                       \
    }                                                                                              \
    void ClassName::deserialize(autil::DataBuffer &dataBuffer) {                                   \
        MEMBERS_MACRO(HA3_DATABUFFER_READ);                                                        \
    }

#define HA3_SERIALIZE_DECLARE_AND_IMPL(MEMBERS_MACRO)                                              \
    void serialize(autil::DataBuffer &dataBuffer) const {                                          \
        MEMBERS_MACRO(HA3_DATABUFFER_WRITE);                                                       \
    }                                                                                              \
    void deserialize(autil::DataBuffer &dataBuffer) {                                              \
        MEMBERS_MACRO(HA3_DATABUFFER_READ);                                                        \
    }
