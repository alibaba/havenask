#ifndef ISEARCH_SEARCHERBIZ_H
#define ISEARCH_SEARCHERBIZ_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/RangeUtil.h>
#include <ha3/search/IndexPartitionWrapper.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/proto/BasicDefs.pb.h>
#include <suez/turing/search/Biz.h>
#include <suez/search/BizMeta.h>
#include <ha3/monitor/SearcherBizMetrics.h>
#include <ha3/config/ConfigAdapter.h>

BEGIN_HA3_NAMESPACE(turing);

class SearcherBiz: public suez::turing::Biz
{
public:
    SearcherBiz();
    ~SearcherBiz();
protected:
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    virtual tensorflow::Status preloadBiz() override;
private:
    SearcherBiz(const SearcherBiz &);
    SearcherBiz& operator=(const SearcherBiz &);
public:
    tensorflow::Status init(const std::string &bizName,
                            const suez::BizMeta &bizMeta,
                            const suez::turing::ProcessResource &processResource)
        override;

protected:
    tensorflow::SessionResourcePtr createSessionResource(uint32_t count) override;
    tensorflow::QueryResourcePtr createQueryResource() override;
    std::string getBizInfoFile() override;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr createIndexSnapshot() override;
    virtual std::string getDefaultGraphDefPath();
    virtual std::string getConfigZoneBizName(const std::string &zoneName);
    virtual std::string getOutConfName(const std::string &confName);
    std::string convertSorterConf();
    std::string convertFunctionConf();
private:
    HA3_NS(service)::SearcherResourcePtr createSearcherResource(
            const HA3_NS(config)::ConfigAdapterPtr &configAdapter);
    bool getRange(uint32_t partCount, uint32_t partId, util::PartitionRange &range);
    proto::PartitionID suezPid2HaPid(const suez::PartitionId& sPid);
    bool getIndexPartition(
            const std::string& tableName,
            const proto::Range &range,
            const suez::TableReader &tableReader,
            std::pair<proto::PartitionID, IE_NAMESPACE(partition)::IndexPartitionPtr> &table);
    search::Cluster2IndexPartitionMapPtr createCluster2IndexPatitionMap(
            const config::ConfigAdapterPtr &configAdapter,
            const std::string &zoneBizName,
            const suez::TableReader &tableReader);
    bool initTimeoutTerminator(int64_t currentTime);
    void clearSnapshot();
protected:
    config::ConfigAdapterPtr _configAdapter;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _snapshotReader;
    int64_t _lastCreateSnapshotTime;
    autil::LoopThreadPtr _clearSnapshotThread;
    mutable autil::ThreadMutex _snapshotLock;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherBiz> SearcherBizPtr;

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEARCHERBIZ_H
