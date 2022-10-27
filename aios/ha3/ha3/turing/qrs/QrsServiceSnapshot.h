#ifndef ISEARCH_QRSSERVICESNAPSHOT_H
#define ISEARCH_QRSSERVICESNAPSHOT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/search/ServiceSnapshot.h>
#include <suez/turing/search/Biz.h>

#include <ha3/proto/BasicDefs.pb.h>
#include <ha3/util/RangeUtil.h>
#include <ha3/turing/qrs/QrsBiz.h>
#include "ha3/turing/qrs/QrsSqlBiz.h"

BEGIN_HA3_NAMESPACE(turing);

class QrsServiceSnapshot:public suez::turing::ServiceSnapshot
{
public:
    QrsServiceSnapshot(bool enableSql);
    ~QrsServiceSnapshot();
private:
    QrsServiceSnapshot(const QrsServiceSnapshot &);
    QrsServiceSnapshot& operator=(const QrsServiceSnapshot &);
protected:
    suez::turing::BizPtr doCreateBiz(const std::string &bizName) override;
    bool doInit(const suez::turing::InitOptions &options,
                suez::turing::ServiceSnapshot *oldSnapshot) override;
    void calculateBizMetas(const suez::BizMetas &currentBizMetas,
                           ServiceSnapshot *oldSnapshot,
                           suez::BizMetas &toUpdate,
                           suez::BizMetas &toKeep) const override;
public:
    std::string getBizName(const std::string &bizName) const override;
    QrsBizPtr getFirstQrsBiz();
	QrsSqlBizPtr getFirstQrsSqlBiz();
    proto::PartitionID getPid() const{
        return _pid;
    }
    std::string getAddress() const{
        return _address;
    }
    void setBasicTuringBizNames(const std::set<std::string> &bizNames) {
        _basicTuringBizNames = bizNames;
    }
private:
    std::vector<std::string> getTopoBizNames(QrsBiz *qrsBiz) const;
    std::string calTopoInfo() override;
    void initAddress();
    bool initPid();
    bool getRange(uint32_t partCount, uint32_t partId, util::PartitionRange &range);
private:
    uint32_t calcVersion(suez::turing::Biz *biz);
    uint32_t calcProtocolVersion(suez::turing::Biz *biz);
private:
    bool _enableSql;
    proto::PartitionID _pid;
    std::string _address;
    std::set<std::string> _basicTuringBizNames;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<QrsServiceSnapshot> QrsServiceSnapshotPtr;

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_QRSSERVICESNAPSHOT_H
