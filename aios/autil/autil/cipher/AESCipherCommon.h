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

#include <string>
#include "autil/Log.h"

namespace autil { namespace cipher {

enum CipherType
{
    CT_AES_128_CBC,
    CT_AES_192_CBC,
    CT_AES_256_CBC,
    CT_AES_128_CTR,
    CT_AES_192_CTR,
    CT_AES_256_CTR,
    CT_UNKNOWN,
};

enum DigistType
{
    DT_MD5,
    DT_SHA256,
    DT_UNKNOWN,
};

struct CipherOption
{
    CipherType type = CT_UNKNOWN;
    DigistType digist = DT_MD5;
    std::string passwd;    
    std::string saltHexString;
    bool needSalt = false;
    bool usePbkdf2 = false;
    std::string keyHexString;  /* key */
    std::string ivHexString;   /* initialized variable */

    CipherOption(CipherType type_) : type(type_) {}

    bool hasSecretKey() const;
    std::string toKVString(bool useBase64);
    bool fromKVString(const std::string& str, bool useBase64);
    bool operator==(const CipherOption& other) const;

    static CipherOption secretKey(CipherType cipherType,
                                  const std::string& keyStr, const std::string& ivStr)
    {
        CipherOption option(cipherType);
        option.keyHexString = keyStr;
        option.ivHexString = ivStr;
        return option;
    }

    static CipherOption password(CipherType cipherType, DigistType dgstType,
                                 std::string passwd, bool usePbkdf2 = false)
    {
        CipherOption option(cipherType);
        option.type = cipherType;
        option.digist = dgstType;
        option.passwd = passwd;
        option.usePbkdf2 = usePbkdf2;
        option.needSalt = false;
        return option;
    }

    static CipherOption passwordWithRandomSalt(CipherType cipherType,
            DigistType dgstType, std::string passwd, bool usePbkdf2 = false)
    {
        auto option = password(cipherType, dgstType, passwd, usePbkdf2);
        option.needSalt = true;
        return option;
    }

    static CipherOption passwordWithSalt(CipherType cipherType,
            DigistType dgstType, std::string passwd, std::string saltHexString,
            bool usePbkdf2 = false)
    {
        auto option = passwordWithRandomSalt(cipherType, dgstType, passwd, usePbkdf2);
        option.saltHexString = saltHexString;
        return option;
    }

    static CipherType parseCipherType(const std::string& typeStr);
    static DigistType parseDigistType(const std::string str);

    static std::string cipherTypeToString(CipherType type);
    static std::string digistTypeToString(DigistType type);
    
private:
    AUTIL_LOG_DECLARE();
};

}}

