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
#include <stddef.h>
#include <iostream>
#include <string>

#include "autil/legacy/base64.h"
#include "autil/legacy/exception.h"

using namespace std;

namespace autil {
namespace legacy {
/*
 * in  xxxxxxxx xxxxxxxx xxxxxxxx
 * out 11111122 22223333 33444444
 */

void Base64Encoding(std::istream& is, std::ostream& os, char makeupChar, const char *alphabet)
{
    int out[4];
    int remain = 0;
    while (!is.eof())
    {
        int byte1 = is.get();
        if (byte1 < 0)
        {
            break;
        }
        int byte2 = is.get();
        int byte3;
        if (byte2 < 0)
        {
            byte2 = 0;
            byte3 = 0;
            remain = 1;
        }
        else
        {
            byte3 = is.get();
            if (byte3 < 0)
            {
                byte3 = 0;
                remain = 2;
            }
        }
        out[0] = static_cast<unsigned>(byte1) >> 2;
        out[1] = ((byte1 & 0x03) << 4) + (static_cast<unsigned>(byte2) >> 4);
        out[2] = ((byte2 & 0x0F) << 2) + (static_cast<unsigned>(byte3) >> 6);
        out[3] = byte3 & 0x3F;

        if (remain == 1)
        {
            os.put(out[0] = alphabet[out[0]]);
            os.put(out[1] = alphabet[out[1]]);
            os.put(makeupChar);
            os.put(makeupChar);
        }
        else if (remain == 2)
        {
            os.put(out[0] = alphabet[out[0]]);
            os.put(out[1] = alphabet[out[1]]);
            os.put(out[2] = alphabet[out[2]]);
            os.put(makeupChar);
        }
        else
        {
            os.put(out[0] = alphabet[out[0]]);
            os.put(out[1] = alphabet[out[1]]);
            os.put(out[2] = alphabet[out[2]]);
            os.put(out[3] = alphabet[out[3]]);
        }
    }
}

//Base64Decoding
void Base64DecodingEx(std::istream& is, std::ostream& os, char makeupChar, char plus, char slash)
{
    int out[3];
    int byte[4];
    int bTmp;
    int bTmpNext;
    const int numOfAlpha = 26;
    const int numOfDecimalNum = 10;
    const int numOfBase64 = 64;
    int index = 0;

    while (is.peek() >= 0)
    {
        byte[0] = byte[1] = byte[2] = byte[3] = 0;
        out[0] = out[1] = out[2] = 0;
        bTmp = 0;

        for (int i = 0; i < 4; i++)
        {
            bTmp = is.get();

            if (bTmp == makeupChar)
            {
                index = i;
                break;
            }
            else if (bTmp >= 'A' && bTmp <= 'Z')
            {
                byte[i] = bTmp - 'A';
            }
            else if (bTmp >= 'a' && bTmp <= 'z')
            {
                byte[i] = bTmp - 'a' + numOfAlpha;
            }
            else if (bTmp >= '0' && bTmp <= '9')
            {
                byte[i] = bTmp + numOfAlpha * 2 - '0';
            }
            else if (bTmp == plus)
            {
                byte[i] = numOfAlpha * 2 + numOfDecimalNum;
            }
            else if (bTmp == slash)
            {
                byte[i] = numOfBase64 - 1;
            }
            else if (bTmp < 0)
            {
                AUTIL_LEGACY_THROW(BadBase64Exception, "\n\nInvaild input!\nInput must be a multiple of four!!\n");
            }
            else
            {
                AUTIL_LEGACY_THROW(BadBase64Exception, "\n\nInvaild input!\nPlease input the character in the string below:\nABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n");
            }
        }

        out[0] = (byte[0] << 2) + (static_cast<unsigned>(byte[1]) >> 4) ;
        out[1] = ((byte[1] & 0x0F) << 4) + (static_cast<unsigned>(byte[2]) >> 2);
        out[2] = ((byte[2] & 0x03) << 6) + byte[3];

        if (bTmp == makeupChar)
        {
            if (index == 0 || index == 1)
            {
                AUTIL_LEGACY_THROW(BadBase64Exception, "\n\nInvaild input!\nInput must be a multiple of four!!\n= must be the third or fourth place in the last four characters!\n");
            }
            else if (index == 2)
            {
                bTmpNext = is.get();
                if (bTmpNext == makeupChar)
                {
                    if (is.peek() < 0)
                    {
                        os.put(out[0]);
                    }
                    else
                    {
                        AUTIL_LEGACY_THROW(BadBase64Exception, "\n\nInvaild input!\nPlease do not append any character after == !\n");
                    }
                }
                else
                {
                    AUTIL_LEGACY_THROW(BadBase64Exception, "\n\nInvaild input!\nPlease do not append any character after = except = !\n");
                }
            }
            else
            {
                if (is.peek() < 0)
                {
                    os.put(out[0]);
                    os.put(out[1]);
                }
                else
                {
                    AUTIL_LEGACY_THROW(BadBase64Exception, "\n\nInvaild input!\nInput must be a multiple of four!!\nPlease do not append any character after the first = !\n");
                }
            }
        }
        else
        {
            os.put(out[0]);
            os.put(out[1]);
            os.put(out[2]);
        }
    }
}

void Base64Decoding(std::istream&is, std::ostream&os,
                                   char plus, char slash)
{
    Base64DecodingEx(is, os, '=', plus, slash);
}

/* aaaack but it's fast and const should make it shared text page. */
static const unsigned char pr2six[256] =
{
    /* ASCII table */
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
    64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
    64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64
};

int Base64decode_len(const char *bufcoded)
{
    int nbytesdecoded;
    const unsigned char *bufin;
    int nprbytes;

    bufin = (const unsigned char *) bufcoded;
    while (pr2six[*(bufin++)] <= 63);

    nprbytes = (bufin - (const unsigned char *) bufcoded) - 1;
    nbytesdecoded = ((nprbytes + 3) / 4) * 3;

    return nbytesdecoded + 1;
}

int Base64decode(char *bufplain, const char *bufcoded)
{
    int nbytesdecoded;
    const unsigned char *bufin;
    unsigned char *bufout;
    int nprbytes;

    bufin = (const unsigned char *) bufcoded;
    while (pr2six[*(bufin++)] <= 63);
    nprbytes = (bufin - (const unsigned char *) bufcoded) - 1;
    nbytesdecoded = ((nprbytes + 3) / 4) * 3;

    bufout = (unsigned char *) bufplain;
    bufin = (const unsigned char *) bufcoded;

    while (nprbytes > 4) {
        *(bufout++) =
            (unsigned char) (pr2six[*bufin] << 2 | pr2six[bufin[1]] >> 4);
        *(bufout++) =
            (unsigned char) (pr2six[bufin[1]] << 4 | pr2six[bufin[2]] >> 2);
        *(bufout++) =
            (unsigned char) (pr2six[bufin[2]] << 6 | pr2six[bufin[3]]);
        bufin += 4;
        nprbytes -= 4;
    }

    /* Note: (nprbytes == 1) would be an error, so just ingore that case */
    if (nprbytes > 1) {
        *(bufout++) =
            (unsigned char) (pr2six[*bufin] << 2 | pr2six[bufin[1]] >> 4);
    }
    if (nprbytes > 2) {
        *(bufout++) =
            (unsigned char) (pr2six[bufin[1]] << 4 | pr2six[bufin[2]] >> 2);
    }
    if (nprbytes > 3) {
        *(bufout++) =
            (unsigned char) (pr2six[bufin[2]] << 6 | pr2six[bufin[3]]);
    }

    *(bufout++) = '\0';
    nbytesdecoded -= (4 - nprbytes) & 3;
    return nbytesdecoded;
}

static const char basis_64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

int Base64encode_len(int len)
{
    return ((len + 2) / 3 * 4) + 1;
}

int Base64encode(char *encoded, const char *string, int len)
{
    int i;
    char *p;

    p = encoded;
    for (i = 0; i < len - 2; i += 3) {
        *p++ = basis_64[(string[i] >> 2) & 0x3F];
        *p++ = basis_64[((string[i] & 0x3) << 4) |
                        ((int) (string[i + 1] & 0xF0) >> 4)];
        *p++ = basis_64[((string[i + 1] & 0xF) << 2) |
                        ((int) (string[i + 2] & 0xC0) >> 6)];
        *p++ = basis_64[string[i + 2] & 0x3F];
    }
    if (i < len) {
        *p++ = basis_64[(string[i] >> 2) & 0x3F];
        if (i == (len - 1)) {
            *p++ = basis_64[((string[i] & 0x3) << 4)];
            *p++ = '=';
        }
        else {
            *p++ = basis_64[((string[i] & 0x3) << 4) |
                            ((int) (string[i + 1] & 0xF0) >> 4)];
            *p++ = basis_64[((string[i + 1] & 0xF) << 2)];
        }
        *p++ = '=';
    }

    *p++ = '\0';
    // do not count \0
    return p - encoded - 1;
}

std::string Base64DecodeFast(const std::string &input) {
    return Base64DecodeFast(input.c_str(), input.size());
}

std::string Base64EncodeFast(const std::string &input) {
    return Base64EncodeFast(input.c_str(), input.size());
}

std::string Base64DecodeFast(const char *data, size_t size) {
    string buffer;
    buffer.resize(size + 1);
    int len = Base64decode(const_cast<char*>(buffer.data()), data);
    buffer.resize(len);
    return buffer;
}

std::string Base64EncodeFast(const char *data, size_t size) {

    string buffer;
    buffer.resize(Base64encode_len(size));
    int len = Base64encode(const_cast<char*>(buffer.data()), data, size);
    buffer.resize(len);
    return buffer;
}


}
}
