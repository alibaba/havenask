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

namespace autil {
namespace cipher {

class AESCipherUtility;
class AESCipherEncrypter {
public:
    AESCipherEncrypter(std::unique_ptr<AESCipherUtility> utility);
    virtual ~AESCipherEncrypter();

public:
    bool encryptMessage(const std::string &plainText, std::string &cipherText);

    std::string getKeyHexString() const;
    std::string getIvHexString() const;
    std::string getSaltHexString() const;

protected:
    std::string toErrorString(unsigned long err);

protected:
    std::unique_ptr<AESCipherUtility> _utility;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cipher
} // namespace autil
