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
 * File name: aneterror.h
 * Author: zhangli
 * Create time: 2008-12-21 15:29:40
 * $Id$
 *
 * Description: ***add description here***
 *
 */

#ifndef ANET_ANETERROR_H_
#define ANET_ANETERROR_H_
//*****add include headers here...
namespace anet {
class AnetError {
public:
    static const int INVALID_DATA;
    static const char *INVALID_DATA_S;

    static const int CONNECTION_CLOSED;
    static const char *CONNECTION_CLOSED_S;

    static const int PKG_TOO_LARGE;
    static const char *PKG_TOO_LARGE_S;

    /**
     * HTTP related error no
     */
    static const int LENGTH_REQUIRED;
    static const char *LENGTH_REQUIRED_S;

    static const int URI_TOO_LARGE;
    static const char *URI_TOO_LARGE_S;

    static const int VERSION_NOT_SUPPORT;
    static const char *VERSION_NOT_SUPPORT_S;

    static const int TOO_MANY_HEADERS;
    static const char *TOO_MANY_HEADERS_S;
};

} /*end namespace anet*/
#endif /*ANET_ANETERROR_H_*/
