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

#include "autil/Log.h"
#include "autil/cipher/AESCipherCommon.h"
#include "autil/cipher/AESCipherEncrypter.h"
#include "autil/cipher/AESCipherDecrypter.h"
#include "autil/cipher/AESCipherStreamEncrypter.h"
#include "autil/cipher/AESCipherStreamDecrypter.h"

namespace autil { namespace cipher {

class AESCipherUtility;

class AESCipherCreator
{
public:
    AESCipherCreator();
    ~AESCipherCreator();

public:
    static std::unique_ptr<AESCipherEncrypter> createEncrypter(CipherOption option);
    static std::unique_ptr<AESCipherDecrypter> createDecrypter(CipherOption option);

    static std::unique_ptr<AESCipherStreamEncrypter> createStreamEncrypter(CipherOption option,
            size_t maxCapacity = std::numeric_limits<size_t>::max(), 
            size_t readTimeoutInMs = 50 /* 50ms, set 0 will return immediately if no data */);
    
    static std::unique_ptr<AESCipherStreamDecrypter> createStreamDecrypter(CipherOption option,
            size_t maxCapacity = std::numeric_limits<size_t>::max(), 
            size_t readTimeoutInMs = 50 /* 50ms, set 0 will return immediately if no data */);
    
private:
    static AESCipherUtility* createUtility(CipherOption option);
    
private:
    AUTIL_LOG_DECLARE();
};

}}

