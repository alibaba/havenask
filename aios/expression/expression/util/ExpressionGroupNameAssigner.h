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
#ifndef ISEARCH_EXPRESSION_EXPRESSIONGROUPNAMEASSIGNER_H
#define ISEARCH_EXPRESSION_EXPRESSIONGROUPNAMEASSIGNER_H

#include "expression/common.h"
#include "autil/StringUtil.h"

namespace expression {

#define TMP_EXPR_GROUP_NAME "__tmp_expr_group_name"

class ExpressionGroupNameAssigner
{
public:
    ExpressionGroupNameAssigner()
    {
        for (uint32_t i = 0; i < 20; i++)
        {
            _fieldGroupNames.push_back(_innerCreateGroupName(i));
        }
    }

    ~ExpressionGroupNameAssigner() {}
public:
    std::string getGroupName(uint32_t groupId) const
    {
        if (likely(groupId < _fieldGroupNames.size()))
        {
            return _fieldGroupNames[groupId];
        }
        return _innerCreateGroupName(groupId);
    }

private:
    static std::string _innerCreateGroupName(uint32_t groupId)
    {
        std::string groupName =
            TMP_EXPR_GROUP_NAME + std::string("_") + autil::StringUtil::toString<uint32_t>(groupId);
        return groupName;
    }
    
private:
    std::vector<std::string> _fieldGroupNames;
};

}

#endif //ISEARCH_EXPRESSION_EXPRESSIONGROUPNAMEASSIGNER_H
