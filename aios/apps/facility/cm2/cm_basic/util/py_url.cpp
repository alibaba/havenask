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
#include "aios/apps/facility/cm2/cm_basic/util/py_url.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace cm_basic {

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
int py_parse_url(const char* url, char* site, int ssize, int* port, char* file, int fsize)
{
    const char* begin = NULL;
    const char* p = NULL;

    assert(url && site && port && file);
    assert(ssize > 0 && fsize > 0);

    site[0] = '\0';
    file[0] = '\0';
    *port = 80;

    if (strncasecmp(url, "http://", 7) == 0) {
        begin = url + 7;
    } else {
        begin = url;
    }

    // get site
    for (p = begin; *p != '\0' && *p != ':' && *p != '/'; p++)
        ;
    int len = p - begin;
    if (len >= ssize) {
        return -1;
    }
    memcpy(site, begin, len);
    site[len] = '\0';
    if (*p == '\0') {
        begin = p;
    } else {
        begin = p + 1;
    }

    // get port
    if (*p == ':') { // has port
        for (p = begin; *p != '/' && *p != '\0'; p++)
            ;
        if (sscanf(begin, "%d", port) != 1) {
            return -1;
        }
    } else {
        *port = 80;
    }

    // get file
    if (*p == '\0') {
        begin = p;
    } else {
        begin = p + 1;
    }
    strncpy(file, begin, fsize - 1);
    file[fsize - 1] = '\0';

    return 0;
}

/*
 * func : get url site
 *
 * args : url, the input url
 *      : site, ssize, site buffer and its size
 *
 * ret  : 0,  succeed
 *      : -1, failed.
 */
int py_get_site(const char* url, char* site, int ssize)
{
    const char* begin = NULL;
    const char* p = NULL;

    if (strncasecmp(url, "http://", 7) == 0) {
        begin = url + 7;
    } else {
        begin = url;
    }

    // get site
    for (p = begin; *p != '\0' && *p != ':' && *p != '/'; p++)
        ;
    int len = p - begin;
    if (len >= ssize) {
        return -1;
    }
    memcpy(site, begin, len);
    site[len] = '\0';

    return 0;
}

/*
 * func : decode url, change %XX%YY into multi-byte word
 *
 * args : src, srclen, source string and its length
 *
 * ret  : -1, error
 *      : else, length of result string
 */
int decode_url(const char* src, int srclen, char* dest, int dsize)
{
    int val = 0;
    int dcnt = 0;
    char tmp[3];

    tmp[2] = '\0';
    int i = 0;

    for (i = 0; i < srclen;) {
        if (dcnt + 1 >= dsize) {
            return -1;
        }
        unsigned char ch = src[i];
        if (ch == '%' && i + 2 < srclen) {
            tmp[0] = src[i + 1];
            tmp[1] = src[i + 2];
            sscanf(tmp, "%x", &val);
            dest[dcnt++] = val;
            i += 3;
        } else {
            if (ch == '+') {
                dest[dcnt++] = ' ';
            } else {
                dest[dcnt++] = ch;
            }
            i += 1;
        }
    }

    dest[dcnt] = 0;

    return dcnt;
}

/*
 * func : encode url, change multi-byte char into %XX; change blank etc. to %20
 *
 * args : src, srclen, srouce string and its length
 *      : dest, dsize, dest buffer and its length
 *
 * ret  : -1, error
 *      : else, length of result string
 */
int encode_url(const char* src, int srclen, char* dest, int dsize)
{
    int dcnt = 0;
    int len = 0;
    unsigned char ch = '\0';
    char tmp[20];
    int i = 0;

    // ups中的特殊符号都做encode: &[#38|0x26] =[#61|0x3D] %[#37|0x25] 空格[#32|0x20] ;[#59|0x3B]
    static unsigned char encode_tab[256] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
        1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    };

    for (i = 0; i < srclen; i++) {
        ch = src[i];

        if (ch > 127) {
            len = sprintf(tmp, "%%%2X", ch);
            if (dcnt + len >= dsize) {
                return -1;
            }
            memcpy(dest + dcnt, tmp, len);
            dcnt += len;
        } else if (encode_tab[ch] == 1) {
            len = sprintf(tmp, "%%%1X", ch);
            if (dcnt + len >= dsize) {
                return -1;
            }
            memcpy(dest + dcnt, tmp, len);
            dcnt += len;

        } else {
            if (dcnt + 1 >= dsize) {
                return -1;
            }
            dest[dcnt++] = ch;
        }
    }

    dest[dcnt] = '\0';
    return dcnt;
}

} // namespace cm_basic
