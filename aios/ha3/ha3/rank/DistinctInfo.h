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
#pragma once

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "indexlib/indexlib.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Trait.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace rank {
class TreeNode;

class DistinctInfo
{
public:
    DistinctInfo() {
        reset(matchdoc::INVALID_MATCHDOC);
    }
    DistinctInfo(matchdoc::MatchDoc matchDoc) {
        assert(matchdoc::INVALID_MATCHDOC != matchDoc);
        reset(matchDoc);
    }
    ~DistinctInfo() {}
public:
    docid_t getDocId() const;

    void setMatchDoc(matchdoc::MatchDoc matchDoc) {
        _matchDoc = matchDoc;
    }

    matchdoc::MatchDoc getMatchDoc() {
        return _matchDoc;
    }

    uint32_t getQueuePosition() const {
        return _queuePosition;
    }
    void setQueuePosition(uint32_t position) {
        _queuePosition = position;
    }

    uint32_t getKeyPosition() const {
        return _keyPosition;
    }
    void setKeyPosition(uint32_t position) {
        _keyPosition = position;
    }

    TreeNode* getTreeNode() {
        return _treeNode;
    }
    void setTreeNode(TreeNode *node) {
        _treeNode = node;
    }
    void reset(matchdoc::MatchDoc matchDoc) {
        _matchDoc = matchDoc;
        _queuePosition = 0;
        _keyPosition = 0;
        _treeNode = NULL;
        _next = NULL;
        _distinctBoost = 0.0f;
        _gradeBoost = 0;
    }
    inline float getDistinctBoost() const;
    inline void setDistinctBoost(float boost);

    inline uint32_t getGradeBoost() const;
    inline void setGradeBoost(uint32_t boost);

public:
    DistinctInfo *_next;
private:
    matchdoc::MatchDoc _matchDoc;
    uint32_t _queuePosition;
    uint32_t _keyPosition;
    float _distinctBoost;
    uint32_t _gradeBoost;
    TreeNode *_treeNode;
private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////
// inline 

inline float DistinctInfo::getDistinctBoost() const {
    return _distinctBoost;
}

inline void DistinctInfo::setDistinctBoost(float boost) {
    _distinctBoost = boost;
}

inline uint32_t DistinctInfo::getGradeBoost() const {
    return _gradeBoost;
}

inline void DistinctInfo::setGradeBoost(uint32_t boost) {
    _gradeBoost = boost;
}

} // namespace rank
} // namespace isearch

NOT_SUPPORT_SERIZLIE_TYPE(isearch::rank::DistinctInfo);

