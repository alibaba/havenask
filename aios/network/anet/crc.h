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
/** @file crc.h
 * Define the interface of crc checksum
 * This file derives from apsara/common/apsaraheader/crc.h
 */

#ifndef ANET_CRC_H
#define ANET_CRC_H

#include <stdint.h>

/// define namespace
namespace anet {

typedef uint32_t (*DoCrc32cImpl)(uint32_t, const uint8_t *, uint64_t);

bool IsSSE42Supported();
DoCrc32cImpl GetDoCrc32cImpl();

const char *GetCrcFuncStr(void);

// OPERATIONS
/** Calculate checksum for an input byte stream,
 * result is stored in checksum pool
 *
 * @param initCrc Initial checksum value
 * @param data Input data to calculate checksum
 * @param length Length of input data
 *
 * @return Calculated checksum
 */
inline uint32_t DoCrc32c(uint32_t initCrc, const uint8_t *data, uint64_t length) {
    static DoCrc32cImpl doCrc32c = GetDoCrc32cImpl();
    return doCrc32c(initCrc, data, length);
}

uint32_t DoCrc32c_Lookup(uint32_t initCrc, const uint8_t *data, uint64_t length);

uint32_t DoCrc32c_Intel(uint32_t initCrc, const uint8_t *data, uint64_t length);

} // namespace anet

#endif
