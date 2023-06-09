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
#ifndef _CRC_H
#define _CRC_H

#ifdef __cplusplus
extern "C" {
#endif

    void init_crc();

    unsigned long long get_crc64(const char *buf, int buflen);
    unsigned int get_crc32(const char *buf, int buflen);

#ifdef __cplusplus
}
#endif
#endif
