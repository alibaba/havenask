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
#include "autil/cipher/AESCipherUtility.h"
#include "openssl/rand.h"
#include "openssl/obj.h"
#include <string.h>

#define CIPHER_NAME(e) OBJ_nid2sn(EVP_CIPHER_nid(e))

namespace autil { namespace cipher {
AUTIL_LOG_SETUP(autil, AESCipherUtility);

AESCipherUtility::AESCipherUtility(CipherType type)
{
    switch (type) {
    case CT_AES_128_CBC: {
        _cipher = EVP_aes_128_cbc();
        break;
    }
    case CT_AES_192_CBC: {
        _cipher = EVP_aes_192_cbc();
        break;
    }
    case CT_AES_256_CBC: {
        _cipher = EVP_aes_256_cbc();
        break;
    }
    case CT_AES_128_CTR: {
        _cipher = EVP_aes_128_ctr();        
        break;
    }
    case CT_AES_192_CTR: {
        _cipher = EVP_aes_192_ctr();
        break;
    }
    case CT_AES_256_CTR: {
        _cipher = EVP_aes_256_ctr();        
        break;
    }
    // TODO: support more mode
    default:
        _cipher = nullptr;
    }
}

AESCipherUtility::~AESCipherUtility() 
{
}

AESCipherUtility& AESCipherUtility::setDigistType(DigistType type)
{
    switch (type) {
    case DT_MD5: {
        _dgst = EVP_md5();
        break;
    }
    case DT_SHA256: {
        _dgst = EVP_sha256();
        break;
    }
    default:
        _dgst = nullptr;
    }
    return *this;
}

AESCipherUtility& AESCipherUtility::enablePbkdf2(int iter)
{
    _usePbkdf2 = true;
    _pbkdf2Iter = (iter <= 0) ? 10000 : iter;
    return *this;
}

AESCipherUtility& AESCipherUtility::enableSalt(bool flag)
{
    _enableSalt = flag;
    return *this;
}

AESCipherUtility& AESCipherUtility::setPassword(const std::string& passwd)
{
    _passwd = passwd;
    return *this;
}

bool AESCipherUtility::setKeyAndIv(const std::string& hKey, const std::string& hiv)
{
    if (_cipher == nullptr) {
        return false;
    }

    int32_t keyLen = hKey.size() / 2;
    int32_t ivLen = hiv.size() / 2;
    if (keyLen > EVP_MAX_KEY_LENGTH || !hex2Bytes(hKey, _key)) {
        AUTIL_LOG(ERROR, "invalid hex key str [%s]", hKey.c_str());
        return false;
    }
    if (ivLen > EVP_MAX_IV_LENGTH || !hex2Bytes(hiv, _iv)) {
        AUTIL_LOG(ERROR, "invalid hex iv str [%s]", hiv.c_str());
        return false;
    }

    if (keyLen != EVP_CIPHER_key_length(_cipher)) {
        AUTIL_LOG(ERROR, "invalid hex key str [%s], key length [%d] not match with cipher [%s]",
                  hKey.c_str(), keyLen, CIPHER_NAME(_cipher));
        return false;
    }
    if (ivLen != EVP_CIPHER_iv_length(_cipher)) {
        AUTIL_LOG(ERROR, "invalid hex iv str [%s], iv length [%d] not match with cipher [%s]",
                  hiv.c_str(), ivLen, CIPHER_NAME(_cipher));
        return false;
    }
    _keyLen = keyLen;
    _ivLen = ivLen;
    return true;
}

bool AESCipherUtility::setSalt(const std::string& hsalt)
{
    if ((hsalt.size() / 2) != SALT_LEN || !hex2Bytes(hsalt, _salt)) {
        AUTIL_LOG(ERROR, "invalid hex salt str [%s]", hsalt.c_str());
        _setSalt = false;
        return false;
    }
    _enableSalt = true;
    _setSalt = true;
    return true;
}

bool AESCipherUtility::setSaltHeader(const std::string& headerStr)
{
    if (headerStr.size() != getSaltHeaderLen() || headerStr.find(magic) != 0) {
        return false;
    }
    memcpy(_salt, headerStr.data() + sizeof(magic) - 1, sizeof(_salt));
    _enableSalt = true;
    _setSalt = true;
    return true;
}

bool AESCipherUtility::hex2Bytes(const std::string& hexStr, unsigned char* bytes) const {
    if ((hexStr.length() % 2) != 0) {
        return false;
    }
    for (int i = 0; i < hexStr.length(); i += 2) {
        int high = hexChar2Int(hexStr[i]);
        int low = hexChar2Int(hexStr[i + 1]);
        if (high == -1 || low == -1) {
            return false;
        }
        bytes[i / 2] = (unsigned char)(high * 16 + low);
    }
    return true;
}

std::string AESCipherUtility::byte2HexStr(const unsigned char* byte, size_t len) const
{
    std::string str;
    str.resize(len * 2);
    char buf[3] = {0};
    for (size_t i = 0; i < len; i++) {
        size_t idx = 2 * i;
        snprintf(buf, 3, "%02X", byte[i]);
        str[idx] = buf[0];
        str[idx + 1] = buf[1];
    }
    return str;
}

bool AESCipherUtility::calculate()
{
    if (_cipher == nullptr) {
        AUTIL_LOG(ERROR, "invalid null cipher.");
        return false;
    }
    if (!needCalculate()) {
        return true;
    }
    if (_passwd.empty()) {
        AUTIL_LOG(ERROR, "invalid empty passwd.");
        return false;
    }
    if (_dgst == nullptr) { // use md5 by default
        _dgst = EVP_md5();
    }
    if (_enableSalt && !_setSalt) {
        if (RAND_bytes(_salt, sizeof(_salt)) <= 0) {
            AUTIL_LOG(ERROR, "make random salt fail.");            
            return false;
        }
    }
    int iklen = EVP_CIPHER_key_length(_cipher);
    int ivlen = EVP_CIPHER_iv_length(_cipher);
    unsigned char* sptr = _enableSalt ? _salt : nullptr;
    if (_usePbkdf2) {
        unsigned char tmpkeyiv[EVP_MAX_KEY_LENGTH + EVP_MAX_IV_LENGTH];
        int islen = (_enableSalt ? sizeof(_salt) : 0);
        if (!PKCS5_PBKDF2_HMAC(_passwd.c_str(), _passwd.size(), sptr, islen,
                               _pbkdf2Iter, _dgst, iklen+ivlen, tmpkeyiv))
        {
            printf("PKCS5_PBKDF2_HMAC failed\n");
            return false;
        }
        memcpy(_key, tmpkeyiv, iklen);
        memcpy(_iv, tmpkeyiv+iklen, ivlen);
    } else {
        EVP_BytesToKey(_cipher, _dgst, sptr,
                       (const unsigned char*)_passwd.c_str(), _passwd.size(),
                       1, _key, _iv);
    }
    _keyLen = iklen;
    _ivLen = ivlen;
    return true;
}

std::string AESCipherUtility::getKeyHexString() const
{
    size_t len = 0;
    const unsigned char* byte = getKey(len);
    return (byte == nullptr) ? "null" : byte2HexStr(byte, len);
}

std::string AESCipherUtility::getIvHexString() const
{
    size_t len = 0;
    const unsigned char* byte = getIv(len);
    return (byte == nullptr) ? "null" : byte2HexStr(byte, len);
}

std::string AESCipherUtility::getSaltHexString() const
{
    size_t len = 0;
    const unsigned char* byte = getSalt(len);
    return (byte == nullptr) ? "null" : byte2HexStr(byte, len);
}

std::string AESCipherUtility::debugString() const
{
    std::string str;
    str += "key[";
    str += getKeyHexString();
    str += "],iv[";
    str += getIvHexString();
    str += "],salt[";
    str += getSaltHexString();
    str += "]";
    return str;
}

const unsigned char* AESCipherUtility::getKey(size_t& len) const
{
    if (_cipher == nullptr || _keyLen != EVP_CIPHER_key_length(_cipher)) {
        return nullptr;
    }
    len = (size_t)_keyLen;
    return _key;
}

const unsigned char* AESCipherUtility::getIv(size_t& len) const
{
    if (_cipher == nullptr || _ivLen != EVP_CIPHER_iv_length(_cipher)) {
        return nullptr;
    }
    len = (size_t)_ivLen;
    return _iv;
}

const unsigned char* AESCipherUtility::getSalt(size_t& len) const
{
    if (!_enableSalt) {
        return nullptr;
    }
    len = sizeof(_salt);
    return _salt;
}

std::string AESCipherUtility::getSaltHeader() const
{
    if (!_enableSalt) {
        return std::string("");
    }
    std::string ret;
    ret.resize(getSaltHeaderLen());
    char* data = (char*)ret.data();
    memcpy(data, magic, sizeof(magic) - 1);
    memcpy(data + sizeof(magic) - 1, _salt, sizeof(_salt));
    return ret;
}

size_t AESCipherUtility::getSaltHeaderLen() const
{
    return sizeof(magic) - 1 + sizeof(_salt);
}

size_t AESCipherUtility::getCipherBlockSize() const
{
    return _cipher ? EVP_CIPHER_block_size(_cipher) : 0;
}

size_t AESCipherUtility::getMaxEncryptDataLength(size_t len) const
{
    size_t blockSize = getCipherBlockSize();
    size_t saltHeaderLen = needSaltHeader() ? getSaltHeaderLen() : 0;
    return ((len + saltHeaderLen + blockSize - 1) / blockSize + 1) * blockSize;
}

}}

