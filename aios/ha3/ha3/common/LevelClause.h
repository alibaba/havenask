#ifndef ISEARCH_LEVELCLAUSE_H
#define ISEARCH_LEVELCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

enum LevelQueryType {
    ONLY_FIRST_LEVEL,
    CHECK_FIRST_LEVEL,
    BOTH_LEVEL,
};

typedef std::vector<std::vector<std::string> > ClusterLists;

class LevelClause : public ClauseBase
{
public:
    LevelClause()
        : _minDocs(0)
        , _levelQueryType(CHECK_FIRST_LEVEL)
        , _useLevel(true)
    {}
    
    ~LevelClause() {}
private:
    LevelClause(const LevelClause &);
    LevelClause& operator=(const LevelClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override {
        return;
    }
    void deserialize(autil::DataBuffer &dataBuffer) override {
        return;
    }
    std::string toString() const override {
        return _originalString;
    }
public:
    bool useLevel() const {
        return _useLevel;
    }
    void setUseLevel(bool f) {
        _useLevel = f;
    }
    LevelQueryType getLevelQueryType() const {
        return _levelQueryType;
    }
    void setLevelQueryType(LevelQueryType levelQueryType) {
        _levelQueryType = levelQueryType;
    }
    const ClusterLists& getLevelClusters() const {
        return _levelClusters;
    }
    void setLevelClusters(const ClusterLists& levelClusters) {
        _levelClusters = levelClusters;
        for (size_t i = 0; i < _levelClusters.size(); ++i) {
            for (size_t j = 0; j < _levelClusters[i].size(); ++j) {
                _levelClusters[i][j] = cluster2ZoneName(_levelClusters[i][j]);
            }
        }
    }
    const std::string& getSecondLevelCluster() const {
        if (_levelClusters.empty() || _levelClusters[0].empty()) {
            return HA3_EMPTY_STRING;
        }
        return _levelClusters[0][0];
    }
    void setSecondLevelCluster(const std::string& cluster) {
        _levelClusters.clear();
        _levelClusters.push_back(std::vector<std::string>(1, cluster2ZoneName(cluster)));
    }
    uint32_t getMinDocs() const {
        return _minDocs;
    }
    void setMinDocs(uint32_t minDocs) {
        _minDocs = minDocs;
    }
    bool maySearchSecondLevel() const {
        return _useLevel
            && (_levelQueryType == CHECK_FIRST_LEVEL
                || _levelQueryType == BOTH_LEVEL);
    }
private:
    ClusterLists _levelClusters;

    uint32_t _minDocs;
    LevelQueryType _levelQueryType;
    bool _useLevel;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LevelClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_LEVELCLAUSE_H
