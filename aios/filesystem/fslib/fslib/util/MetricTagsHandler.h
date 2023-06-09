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
#ifndef FSLIB_METRICTAGSHANDLER_H
#define FSLIB_METRICTAGSHANDLER_H

#include "fslib/common/common_define.h"
#include "kmonitor/client/core/MetricsTags.h"

FSLIB_BEGIN_NAMESPACE(util);

class MetricTagsHandler
{
public:
    MetricTagsHandler();
    virtual ~MetricTagsHandler();
private:
    MetricTagsHandler(const MetricTagsHandler &);
    MetricTagsHandler& operator = (const MetricTagsHandler &);

public:
    virtual void getTags(const std::string& filePath, kmonitor::MetricsTags& tags) const;

private:
    static std::string PROXY_BEGIN;
    static std::string PROXY_SEP;
    static std::string PARAMS_SEP;
    static char INVAILD_CHAR;
    static char ESCAPE_CHAR;
};

FSLIB_TYPEDEF_SHARED_PTR(MetricTagsHandler);

FSLIB_END_NAMESPACE(util);

#endif //FSLIB_METRICTAGSHANDLER_H
