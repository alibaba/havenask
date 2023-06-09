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
/**
 * File name: aneterror.cpp
 * Author: zhangli
 * Create time: 2008-12-21 15:29:40
 * $Id$
 * 
 * Description: ***add description here***
 * 
 */

#include "aios/network/anet/aneterror.h"

namespace anet {
const int AnetError::INVALID_DATA = 1;
const char* AnetError::INVALID_DATA_S = "Invalid Data";

const int AnetError::CONNECTION_CLOSED = 2;
const char* AnetError::CONNECTION_CLOSED_S = "Connection closed";

const int AnetError::PKG_TOO_LARGE = 3;
const char* AnetError::PKG_TOO_LARGE_S = "Packet Too Large";

const int AnetError::LENGTH_REQUIRED = 411;
const char* AnetError::LENGTH_REQUIRED_S = "Length Required";

const int AnetError::URI_TOO_LARGE = 414;
const char* AnetError::URI_TOO_LARGE_S = "Request-URI Too Long";

const int AnetError::VERSION_NOT_SUPPORT = 505;
const char* AnetError::VERSION_NOT_SUPPORT_S = "HTTP Version Not Supported";

const int AnetError::TOO_MANY_HEADERS = 499;
const char* AnetError::TOO_MANY_HEADERS_S = "Too Many Headers";

}/*end namespace anet*/
