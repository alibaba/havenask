#ifndef ISEARCH_DISTINCTINFO_H
#define ISEARCH_DISTINCTINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <assert.h>
BEGIN_HA3_NAMESPACE(rank);
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
    HA3_LOG_DECLARE();
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

END_HA3_NAMESPACE(rank);

NOT_SUPPORT_SERIZLIE_TYPE(isearch::rank::DistinctInfo);

#endif //ISEARCH_SCOREDOCITEM_H
