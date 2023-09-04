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
#ifndef ISEARCH_MULTI_CALL_MISCUTIL_H
#define ISEARCH_MULTI_CALL_MISCUTIL_H

#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

class MiscUtil
{
public:
    MiscUtil();
    ~MiscUtil();

private:
    MiscUtil(const MiscUtil &);
    MiscUtil &operator=(const MiscUtil &);

public:
    static std::string createBizName(const std::string &cluster, const std::string &biz);
    static float scaleLinear(uint32_t begin, uint32_t end, float pos);
    static std::string getBackTrace();

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(MiscUtil);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_MISCUTIL_H
