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
#include "fslib/util/MetricTagsHandler.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace kmonitor;

FSLIB_BEGIN_NAMESPACE(util);

string MetricTagsHandler::PROXY_BEGIN = "://";
string MetricTagsHandler::PROXY_SEP = "/";
string MetricTagsHandler::PARAMS_SEP = "?";
char MetricTagsHandler::INVAILD_CHAR = ':';
char MetricTagsHandler::ESCAPE_CHAR = '-';

MetricTagsHandler::MetricTagsHandler() {}

MetricTagsHandler::~MetricTagsHandler() {}

void MetricTagsHandler::getTags(const string &filePath, MetricsTags &tags) const {
    size_t found = filePath.find(PROXY_BEGIN);
    if (found == string::npos) {
        tags.AddTag(FSLIB_METRIC_TAGS_DFS_TYPE, FSLIB_FS_LOCAL_FILESYSTEM_NAME);
        return;
    }

    string type = filePath.substr(0, found);
    found = found + PROXY_BEGIN.size();
    tags.AddTag(FSLIB_METRIC_TAGS_DFS_TYPE, type);
    size_t paramsEnd = filePath.find(PROXY_SEP, found);
    size_t paramsBegin = filePath.find(PARAMS_SEP, found);
    string dfsName;
    if (paramsBegin == string::npos || paramsBegin > paramsEnd) {
        dfsName = filePath.substr(found, paramsEnd - found);
    } else {
        dfsName = filePath.substr(found, paramsBegin - found);
    }

    if (dfsName.empty()) {
        return;
    }
    autil::StringUtil::replace(dfsName, MetricTagsHandler::INVAILD_CHAR, MetricTagsHandler::ESCAPE_CHAR);
    tags.AddTag(FSLIB_METRIC_TAGS_DFS_NAME, dfsName);
}

FSLIB_END_NAMESPACE(util);
