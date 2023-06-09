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
#ifndef __INDEXLIB_SOURCE_DEFINE_H
#define __INDEXLIB_SOURCE_DEFINE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SourceDefine
{
public:
    SourceDefine();
    ~SourceDefine();

public:
    static std::string GetDataDir(groupid_t groupId);

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_SOURCE_DEFINE_H
