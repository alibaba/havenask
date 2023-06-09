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
#ifndef FSLIB_URLPARSER_H
#define FSLIB_URLPARSER_H

#include <tuple>
#include "fslib/common/common_define.h"
#include "autil/ConstString.h"

FSLIB_BEGIN_NAMESPACE(util);

class URLParser
{
public:
    using Slice = autil::StringView;

    // rawPath is parameterized, such as, pangu://ea120cloudpangu?app=0321^BIGFILE_APPNAME/root/
    // purePath is removed parameters, such as, pangu://ea120cloudpangu/root/
    // configStr is parameters, such as, app=0321^BIGFILE_APPNAME
    static std::string getPurePath(const std::string& rawPath, std::string* paramsStr = NULL);
    static std::string appendTrailingSlash(const std::string& dirPath);
    static std::string trimTrailingSlash(const std::string& path);
    static std::string normalizePath(const std::string& path);

    // <scheme>://<netloc>?<params><path> into [scheme, netloc, params, path]
    static std::tuple<Slice, Slice, Slice, Slice> splitPath(const std::string& rawPath);

    // overwrite=true will rm all key in rawPath first, otherwise append only
    static std::string appendParam(const std::string& rawPath, const std::string& key,
                                   const std::string& value, bool overwrite = false);
    static std::string eraseParam(const std::string& rawPath, const std::string& key);

private:
    static std::string assemblePath(const Slice& scheme, const Slice& netloc,
                                    const Slice& params, const Slice& path);
public:
    static const constexpr char* PARAMS_CONFIG_SEPARATOR = "?";
    static const constexpr char* PARAMS_KV_SEPARATOR = "=";
    static const constexpr char* PARAMS_KV_PAIR_SEPARATOR = "&";
    static const constexpr char* NETLOC_SEPARATOR = "://";
};

FSLIB_END_NAMESPACE(util);

#endif // FSLIB_URLPARSER_H
