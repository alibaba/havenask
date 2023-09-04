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

/***********************************************************************************
 * Descri   : url parse functions
 *
 * Author   : Paul Yang, zhenahoji@gmail.com
 *
 * Create   : 2009-10-19
 *
 * Update   : 2009-10-19
 **********************************************************************************/
#ifndef __PY_URL_H_
#define __PY_URL_H_

namespace cm_basic {

#ifdef __cplusplus
extern "C" {
#endif

/*
 * func : parse url
 *
 * args : url, the input url
 *      : site, ssize, url site buffer and its size
 *      : port, url port
 *      : file, fsize, url file buffer and its size
 *
 * ret  : 0, succeed
 *      : else, failed.
 */
int py_parse_url(const char* url, char* site, int ssize, int* port, char* file, int fsize);

/*
 * func : get url site
 *
 * args : url, the input url
 *      : site, ssize, site buffer and its size
 *
 * ret  : 0,  succeed
 *      : -1, failed.
 */
int py_get_site(const char* url, char* site, int ssize);

/*
 * func : decode url, change %XX%YY into multi-byte word
 *
 * args : src, srclen, source string and its length
 *
 * ret  : -1, error
 *      : else, length of result string
 */

int decode_url(const char* src, int srclen, char* dest, int dsize);
int encode_url(const char* src, int srclen, char* dest, int dsize);

#ifdef __cplusplus
}
#endif

} // namespace cm_basic
#endif
