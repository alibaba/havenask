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
#ifndef __INDEXLIB_VIRTUAL_ATTRIBUTE_CONTAINER_H
#define __INDEXLIB_VIRTUAL_ATTRIBUTE_CONTAINER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

class VirtualAttributeContainer
{
public:
    VirtualAttributeContainer();
    ~VirtualAttributeContainer();

public:
    void UpdateUsingAttributes(const std::vector<std::pair<std::string, bool>>& attributes);
    void UpdateUnusingAttributes(const std::vector<std::pair<std::string, bool>>& attributes);
    const std::vector<std::pair<std::string, bool>>& GetUsingAttributes();
    const std::vector<std::pair<std::string, bool>>& GetUnusingAttributes();

private:
    std::vector<std::pair<std::string, bool>> mUsingAttributes;
    std::vector<std::pair<std::string, bool>> mUnusingAttributes;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VirtualAttributeContainer);
}} // namespace indexlib::partition

#endif //__INDEXLIB_VIRTUAL_ATTRIBUTE_CONTAINER_H
