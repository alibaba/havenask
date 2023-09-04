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
#include "autil/cipher/AESCipherCreator.h"

#include "autil/cipher/AESCipherUtility.h"

namespace autil {
namespace cipher {
AUTIL_LOG_SETUP(autil, AESCipherCreator);

AESCipherCreator::AESCipherCreator() {}

AESCipherCreator::~AESCipherCreator() {}

std::unique_ptr<AESCipherEncrypter> AESCipherCreator::createEncrypter(CipherOption option) {
    std::unique_ptr<AESCipherUtility> utility(createUtility(option));
    if (utility == nullptr) {
        return nullptr;
    }
    return std::make_unique<AESCipherEncrypter>(std::move(utility));
}

std::unique_ptr<AESCipherDecrypter> AESCipherCreator::createDecrypter(CipherOption option) {
    std::unique_ptr<AESCipherUtility> utility(createUtility(option));
    if (utility == nullptr) {
        return nullptr;
    }
    return std::make_unique<AESCipherDecrypter>(std::move(utility));
}

std::unique_ptr<AESCipherStreamEncrypter>
AESCipherCreator::createStreamEncrypter(CipherOption option, size_t maxCapacity, size_t readTimeoutInMs) {
    std::unique_ptr<AESCipherUtility> utility(createUtility(option));
    if (utility == nullptr) {
        return nullptr;
    }
    return std::make_unique<AESCipherStreamEncrypter>(std::move(utility), maxCapacity, readTimeoutInMs);
}

std::unique_ptr<AESCipherStreamDecrypter>
AESCipherCreator::createStreamDecrypter(CipherOption option, size_t maxCapacity, size_t readTimeoutInMs) {
    std::unique_ptr<AESCipherUtility> utility(createUtility(option));
    if (utility == nullptr) {
        return nullptr;
    }
    return std::make_unique<AESCipherStreamDecrypter>(std::move(utility), maxCapacity, readTimeoutInMs);
}

AESCipherUtility *AESCipherCreator::createUtility(CipherOption option) {
    if (option.type == CipherType::CT_UNKNOWN) {
        AUTIL_LOG(ERROR, "unknown cipher type.");
        return nullptr;
    }
    auto utility = std::make_unique<AESCipherUtility>(option.type);
    if (option.hasSecretKey()) {
        if (!utility->setKeyAndIv(option.keyHexString, option.ivHexString)) {
            return nullptr;
        }
        return utility.release();
    }

    if (option.digist == DigistType::DT_UNKNOWN) {
        AUTIL_LOG(ERROR, "unknown digist type.");
        return nullptr;
    }
    if (option.passwd.empty()) {
        AUTIL_LOG(ERROR, "empty password.");
        return nullptr;
    }
    utility->setDigistType(option.digist).setPassword(option.passwd).enableSalt(option.needSalt);
    if (!option.saltHexString.empty() && !utility->setSalt(option.saltHexString)) {
        return nullptr;
    }
    if (option.usePbkdf2) {
        utility->enablePbkdf2();
    }
    return utility.release();
}

} // namespace cipher
} // namespace autil
