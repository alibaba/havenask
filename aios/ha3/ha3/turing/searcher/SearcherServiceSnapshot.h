#ifndef ISEARCH_SEARCHERSERVICESNAPSHOT_H
#define ISEARCH_SEARCHERSERVICESNAPSHOT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/search/ServiceSnapshot.h>
#include <suez/turing/search/Biz.h>
BEGIN_HA3_NAMESPACE(turing);

class SearcherServiceSnapshot:public suez::turing::ServiceSnapshot
{
public:
    SearcherServiceSnapshot();
    ~SearcherServiceSnapshot();
private:
    SearcherServiceSnapshot(const SearcherServiceSnapshot &);
    SearcherServiceSnapshot& operator=(const SearcherServiceSnapshot &);
public:
    void setBasicTuringBizNames(const std::set<std::string> &bizNames) {
        _basicTuringBizNames = bizNames;
    }
    void setDefaultAgg(const std::string &defaultAggStr) {
        _defaultAggStr = defaultAggStr;
    }
    void setParaWays(const std::string &paraWayStr) {
        _paraWaysStr = paraWayStr;
    }
protected:
    suez::turing::BizPtr doCreateBiz(const std::string &bizName) override;
    std::string getBizName(const std::string &bizName) const override;
    std::string calTopoInfo() override;
    bool doInit(const suez::turing::InitOptions &options,
                suez::turing::ServiceSnapshot *oldSnapshot) override;
    void calculateBizMetas(const suez::BizMetas &currentBizMetas,
                           ServiceSnapshot *oldSnapshot,
                           suez::BizMetas &toUpdate,
                           suez::BizMetas &toKeep) const override;
private:
    std::string getFullVersionStr(const suez::IndexProvider *indexProvider) const;
    suez::BizMetas addExtraBizMetas(const suez::BizMetas &currentBizMetas) const;
    int64_t calcVersion(suez::turing::Biz *biz);
    uint32_t calcProtocolVersion(suez::turing::Biz *biz);
private:
    std::string _fullVersionStr;
    std::set<std::string> _basicTuringBizNames;
    std::string _defaultAggStr;
    std::string _paraWaysStr;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherServiceSnapshot> SearcherServiceSnapshotPtr;

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEARCHERSERVICESNAPSHOT_H
