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
#include "autil/cipher/AESCipherCommon.h"

#include <string.h>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/legacy/base64.h"

namespace autil {
namespace cipher {
AUTIL_LOG_SETUP(autil, CipherOption);

CipherType CipherOption::parseCipherType(const std::string &typeStr) {
    if (typeStr == "aes-128-cbc") {
        return CipherType::CT_AES_128_CBC;
    }
    if (typeStr == "aes-192-cbc") {
        return CipherType::CT_AES_192_CBC;
    }
    if (typeStr == "aes-256-cbc") {
        return CipherType::CT_AES_256_CBC;
    }
    if (typeStr == "aes-128-ctr") {
        return CipherType::CT_AES_128_CTR;
    }
    if (typeStr == "aes-192-ctr") {
        return CipherType::CT_AES_192_CTR;
    }
    if (typeStr == "aes-256-ctr") {
        return CipherType::CT_AES_256_CTR;
    }
    return CipherType::CT_UNKNOWN;
}

DigistType CipherOption::parseDigistType(const std::string str) {
    if (str == "md5") {
        return DigistType::DT_MD5;
    }
    if (str == "sha256") {
        return DigistType::DT_SHA256;
    }
    return DigistType::DT_UNKNOWN;
}

bool CipherOption::operator==(const CipherOption &other) const {
    if (type != other.type) {
        return false;
    }
    if (hasSecretKey() != other.hasSecretKey()) {
        return false;
    }
    if (hasSecretKey()) {
        return keyHexString == other.keyHexString && ivHexString == other.ivHexString;
    }
    return passwd == other.passwd && digist == other.digist && saltHexString == other.saltHexString &&
           needSalt == other.needSalt && usePbkdf2 == other.usePbkdf2;
}

std::string CipherOption::cipherTypeToString(CipherType type) {
    switch (type) {
    case CipherType::CT_AES_128_CBC:
        return std::string("aes-128-cbc");
    case CipherType::CT_AES_192_CBC:
        return std::string("aes-192-cbc");
    case CipherType::CT_AES_256_CBC:
        return std::string("aes-256-cbc");
    case CipherType::CT_AES_128_CTR:
        return std::string("aes-128-ctr");
    case CipherType::CT_AES_192_CTR:
        return std::string("aes-192-ctr");
    case CipherType::CT_AES_256_CTR:
        return std::string("aes-256-ctr");
    default:
        return std::string("unknown");
    }
    return std::string("unknown");
}

std::string CipherOption::digistTypeToString(DigistType type) {
    switch (type) {
    case DigistType::DT_MD5:
        return std::string("md5");
    case DigistType::DT_SHA256:
        return std::string("sha256");
    default:
        return std::string("unknown");
    }
    return std::string("unknown");
}

bool CipherOption::hasSecretKey() const { return !keyHexString.empty() && !ivHexString.empty(); }

std::string CipherOption::toKVString(bool useBase64) {
    std::vector<std::string> kvStrVec;
    if (type != CipherType::CT_UNKNOWN) {
        std::string value = "type=" + cipherTypeToString(type);
        kvStrVec.push_back(value);
    }
    if (hasSecretKey()) {
        std::string value = "key=";
        if (useBase64) {
            value += legacy::Base64EncodeFast(keyHexString.c_str(), keyHexString.size());
        } else {
            value += keyHexString;
        }
        kvStrVec.push_back(value);
        value = "iv=";
        if (useBase64) {
            value += legacy::Base64EncodeFast(ivHexString.c_str(), ivHexString.size());
        } else {
            value += ivHexString;
        }
        kvStrVec.push_back(value);
        return autil::StringUtil::toString(kvStrVec, ";");
    }

    if (digist != DigistType::DT_UNKNOWN) {
        std::string value = "digist=" + digistTypeToString(digist);
        kvStrVec.push_back(value);
    }
    if (!passwd.empty()) {
        std::string value = "passwd=";
        if (useBase64) {
            value += legacy::Base64EncodeFast(passwd.c_str(), passwd.size());
        } else {
            value += passwd;
        }
        kvStrVec.push_back(value);
    }
    if (!saltHexString.empty()) {
        std::string value = "salt=";
        if (useBase64) {
            value += legacy::Base64EncodeFast(saltHexString.c_str(), saltHexString.size());
        } else {
            value += saltHexString;
        }
        kvStrVec.push_back(value);
    }

    if (needSalt) {
        kvStrVec.push_back("need_salt=true");
    }
    if (usePbkdf2) {
        kvStrVec.push_back("use_pbkdf2=true");
    }
    return autil::StringUtil::toString(kvStrVec, ";");
}

bool CipherOption::fromKVString(const std::string &str, bool useBase64) {
    std::vector<std::string> kvStrVec;
    autil::StringUtil::fromString(str, kvStrVec, ";");
    for (auto kvStr : kvStrVec) {
        std::string key;
        std::string value;
        autil::StringUtil::getKVValue(kvStr, key, value, "=", true);
        if (key.empty() || value.empty()) {
            AUTIL_LOG(ERROR, "invalid kv parameter [%s]", kvStr.c_str());
            return false;
        }
        if (key == "key") {
            if (useBase64) {
                value = legacy::Base64DecodeFast(value.c_str(), value.size());
            }
            keyHexString = value;
            continue;
        }
        if (key == "iv") {
            if (useBase64) {
                value = legacy::Base64DecodeFast(value.c_str(), value.size());
            }
            ivHexString = value;
            continue;
        }
        if (key == "passwd") {
            if (useBase64) {
                value = legacy::Base64DecodeFast(value.c_str(), value.size());
            }
            passwd = value;
            continue;
        }
        if (key == "type") {
            type = parseCipherType(value);
            continue;
        }
        if (key == "digist") {
            digist = parseDigistType(value);
            continue;
        }
        if (key == "salt") {
            if (useBase64) {
                value = legacy::Base64DecodeFast(value.c_str(), value.size());
            }
            saltHexString = value;
            continue;
        }
        if (key == "need_salt") {
            autil::StringUtil::toLowerCase(value);
            needSalt = (value == "true");
            continue;
        }
        if (key == "use_pbkdf2") {
            autil::StringUtil::toLowerCase(value);
            usePbkdf2 = (value == "true");
            continue;
        }
        AUTIL_LOG(ERROR, "invalid kv parameter [%s], unknown key [%s]", kvStr.c_str(), key.c_str());
        return false;
    }
    return true;
}

} // namespace cipher
} // namespace autil
