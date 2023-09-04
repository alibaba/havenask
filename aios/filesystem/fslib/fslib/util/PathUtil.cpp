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
#include "fslib/util/PathUtil.h"

#include "fslib/util/URLParser.h"

using namespace std;

FSLIB_BEGIN_NAMESPACE(util);
AUTIL_DECLARE_AND_SETUP_LOGGER(util, PathUtil);

PathUtil::PathUtil() {}

PathUtil::~PathUtil() {}

string PathUtil::normalizePath(const string &path) { return URLParser::normalizePath(path); }

bool PathUtil::isInRootPath(const string &normPath, const string &normRootPath) {
    size_t prefixLen = normRootPath.size();
    if (!normRootPath.empty() && *normRootPath.rbegin() == '/') {
        prefixLen--;
    }

    if (normPath.size() < normRootPath.size() || normPath.compare(0, normRootPath.size(), normRootPath) != 0 ||
        (normPath.size() > normRootPath.size() && normPath[prefixLen] != '/')) {
        return false;
    }
    return true;
}

string PathUtil::joinPath(const string &path, const string &name) {
    if (path.empty()) {
        return name;
    }

    if (*(path.rbegin()) != '/') {
        return path + "/" + name;
    }
    return path + name;
}

string PathUtil::GetParentDirPath(const string &path) {
    string::const_reverse_iterator it;
    for (it = path.rbegin(); it != path.rend() && *it == '/'; it++)
        ;
    for (; it != path.rend() && *it != '/'; it++)
        ;
    for (; it != path.rend() && *it == '/'; it++)
        ;
    return path.substr(0, path.rend() - it);
}

string PathUtil::GetFileName(const string &path) {
    string::const_reverse_iterator it;
    for (it = path.rbegin(); it != path.rend() && *it != '/'; it++)
        ;
    return path.substr(path.rend() - it);
}

bool PathUtil::rewriteToDcachePath(const string &source, string &target) {
    static string kDcachePrefix("dcache://");
    static string kDcacheClusterName("dcache");
    static string kSeperator("://");
    size_t index = source.find(kDcachePrefix);
    if (index != string::npos) {
        target = source;
        AUTIL_LOG(INFO, "source path [%s] is already dcache path, use it directly", source.c_str());
        return true;
    }
    index = source.find(kSeperator);
    if (index == string::npos) {
        AUTIL_LOG(INFO, "source path [%s] is not rewritable, not rewrite", source.c_str());
        return false;
    }
    target = kDcachePrefix + kDcacheClusterName + "/" + source.substr(0, index) + "/" +
             source.substr(index + kSeperator.size());
    AUTIL_LOG(INFO, "source path [%s] rewrite to target path [%s]", source.c_str(), target.c_str());
    return true;
}

FSLIB_END_NAMESPACE(util);
