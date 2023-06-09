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

#include <stddef.h>
#include <iostream>
#include <string>

#include "autil/legacy/exception.h"

namespace autil {
namespace legacy {

class BadBase64Exception : public ExceptionBase
{
public:
    AUTIL_LEGACY_DEFINE_EXCEPTION(BadBase64Exception, ExceptionBase);
};

void Base64Encoding(std::istream&, std::ostream&, char makeupChar = '=',
                    const char *alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/");
void Base64Decoding(std::istream&, std::ostream&, char plus = '+', char slash = '/');
void Base64DecodingEx(std::istream&, std::ostream&, char makeupChar = '=', char plus = '+', char slash = '/');

std::string Base64DecodeFast(const std::string &str);
std::string Base64EncodeFast(const std::string &str);

std::string Base64DecodeFast(const char *data, size_t size);
std::string Base64EncodeFast(const char *data, size_t size);

}
}
