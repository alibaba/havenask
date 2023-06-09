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
#include "autil/StringUtil.h"
#include "autil/Log.h"
#include "fslib/util/URLParser.h"

using namespace std;
using namespace autil;

FSLIB_BEGIN_NAMESPACE(util);
AUTIL_DECLARE_AND_SETUP_LOGGER(util, URLParser);

using Slice = URLParser::Slice;

string URLParser::getPurePath(const string& rawPath, string* paramsStr)
{
    // Example: pangu://ea120cloudpangu?app=0321^BIGFILE_APPNAME/root/
    Slice scheme, netloc, params, path;
    tie(scheme, netloc, params, path) = splitPath(rawPath);
    if (scheme.empty()) {
        return string(path.data(), path.size());
    }    
    if (paramsStr) {
        paramsStr->assign(params.data(), params.size());
    }
    string ret;
    ret.reserve(scheme.size() + 3 + netloc.size() + path.size());
    ret.append(scheme.data(), scheme.size());
    ret.append("://");
    ret.append(netloc.data(), netloc.size());
    ret.append(path.data(), path.size());
    return ret;
}

string URLParser::appendTrailingSlash(const string& dirPath)
{
    string ret(dirPath);
    if (*ret.rbegin() != '/') {
        ret.append(1, '/');
    }
    return ret;
}

string URLParser::trimTrailingSlash(const string& path)
{
    string ret(path);
    ret.erase(ret.find_last_not_of('/') + 1);
    return ret;
}

string URLParser::normalizePath(const string& path)
{
    // Example: dfs://cluster/ -> dfs://cluster//
    // Example: LOCAL://cluster -> LOCAL://cluster
    // Example: LOCAL:///home/ -> LOCAL:///home/
    // Example: LOCAL:////home/ -> LOCAL:///home/
    // Example: ////home/ -> /home/
    
    string ret;
    ret.reserve(path.size());
    char lastChar = 0;
    string::size_type pathBegin = 0;
    string::size_type nameBegin = path.find("://");
    if (nameBegin != string::npos) {
        nameBegin += 3; // "://"
        pathBegin = nameBegin + 1; // '/' or first char of name
        ret.append(path, 0, pathBegin);
        lastChar = path[nameBegin];
    }
    for (size_t i = pathBegin; i < path.size(); ++i)
    {
        if (lastChar == '/' && path[i] == '/') {
            continue;
        }
        lastChar = path[i];
        ret.append(1, lastChar);
    }
    return ret;
}

tuple<Slice, Slice, Slice, Slice> URLParser::splitPath(const std::string& rawPath)
{
    // Example: scheme://c1?app=0321^BIGFILE_APPNAME/root/
    // SCHEME://NAME?PARAMS/PATH
    string::size_type nameBegin = rawPath.find("://");
    if (nameBegin == string::npos) {
        return make_tuple(Slice("", 0ul), Slice("", 0ul),
                          Slice("", 0ul), Slice(rawPath));
    }

    Slice scheme(rawPath.data(), nameBegin);
    nameBegin += 3; // "://"

    string::size_type pathBegin = rawPath.find('/', nameBegin);
    if (pathBegin == string::npos) {
        // Example: scheme://c1, scheme://c1?min=1
        pathBegin = rawPath.size();
    }
    Slice params("", 0ul);
    const char* configPtr = char_traits<char>::find(
            rawPath.data() + nameBegin, pathBegin - nameBegin, '?');
    string::size_type nameEnd = string::npos;
    if (configPtr) {
        // Example: scheme://c1?min=1/x, schemem://c1?min=1
        nameEnd = configPtr - rawPath.data();
        params = {configPtr + 1, (std::size_t)(rawPath.data() + pathBegin - configPtr - 1)};
    } else {
        // Example: scheme://c1/x, scheme://c1, scheme://c1/x?a=1
        nameEnd = pathBegin;
    }
    Slice netloc(rawPath.data() + nameBegin, nameEnd - nameBegin);
    Slice path(rawPath.data() + pathBegin, rawPath.size() - pathBegin);
    return make_tuple(scheme, netloc, params, path);
}

std::string URLParser::appendParam(const string& rawPath, const string& key,
                                   const string& value, bool overwrite)
{
    Slice scheme, netloc, params, path;
    tie(scheme, netloc, params, path) = splitPath(rawPath);

    AUTIL_LOG(DEBUG, "%s => [%s, %s, %s, %s]", rawPath.c_str(),
              scheme.to_string().c_str(), netloc.to_string().c_str(),
              params.to_string().c_str(), path.to_string().c_str());
    if (scheme.empty()) {
        AUTIL_LOG(ERROR, "addParam for path[%s] fail", rawPath.c_str());
        return "";
    }
    
    vector<vector<string>> kvVec;
    StringUtil::fromString(string(params.data(), params.size()), kvVec, "=", "&");
    if (overwrite) {
        kvVec.erase(remove_if(kvVec.begin(), kvVec.end(),
                              [&key](const vector<string>& kv){return kv[0] == key;}),
                    kvVec.end());
    }
    vector<string> newKV{key, value};
    kvVec.emplace_back(std::move(newKV));
    string newParams = StringUtil::toString(kvVec, "=", "&");
    return assemblePath(scheme, netloc, Slice(newParams), path);
}

std::string URLParser::eraseParam(const string& rawPath, const string& key)
{
    Slice scheme, netloc, params, path;
    tie(scheme, netloc, params, path) = splitPath(rawPath);
    if (scheme.empty()) {
        AUTIL_LOG(ERROR, "rmParam for path[%s] fail", rawPath.c_str());
        return "";
    }
    
    vector<vector<string>> kvVec;
    StringUtil::fromString(string(params.data(), params.size()), kvVec, "=", "&");
    kvVec.erase(remove_if(kvVec.begin(), kvVec.end(),
                          [&key](const vector<string>& kv){return kv[0] == key;}),
                kvVec.end());
    string newParams = StringUtil::toString(kvVec, "=", "&");
    return assemblePath(scheme, netloc, Slice(newParams), path);
}

std::string URLParser::assemblePath(const Slice& scheme, const Slice& netloc,
                                    const Slice& params, const Slice& path)
{
    string ret;
    ret.reserve(scheme.size() + 3 + netloc.size() + 
                (params.empty() ? 0 : (1 + params.size())) +
                path.size());
    ret.append(scheme.data(), scheme.size());
    ret.append("://"); // 3
    ret.append(netloc.data(), netloc.size());
    if (!params.empty()) {
        ret.append("?");  // 1
        ret.append(params.data(), params.size());
    }
    ret.append(path.data(), path.size());
    return ret;
}

FSLIB_END_NAMESPACE(util);
