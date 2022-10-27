#ifndef ISEARCH_QRSARPCSESSION_H
#define ISEARCH_QRSARPCSESSION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/Session.h>
#include <google/protobuf/stubs/common.h>
#include <ha3/proto/QrsService.pb.h>
#include <ha3/proto/ProtoClassUtil.h>
#include <ha3/util/HaCompressUtil.h>
#include <ha3/monitor/Ha3BizMetrics.h>
#include <arpc/MessageCodec.h>
#include <arpc/RPCServerAdapter.h>
#include <ha3/service/QrsSessionSearchRequest.h>
#include <ha3/service/QrsSessionSearchResult.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/turing/qrs/QrsServiceSnapshot.h>
#include <ha3/turing/qrs/QrsRunGraphContext.h>
#include <suez/turing/common/PoolCache.h>
#include <suez/turing/common/RunIdAllocator.h>

BEGIN_HA3_NAMESPACE(service);

class QrsArpcSession : public Session
{
public:
    QrsArpcSession(SessionPool* pool = NULL);
    virtual ~QrsArpcSession();
private:
    QrsArpcSession(const QrsArpcSession &);
    QrsArpcSession& operator=(const QrsArpcSession &);
public:
    void processRequest();
    void dropRequest();
    bool beginSession();
    void setRequest(const proto::QrsRequest *request,
                    proto::QrsResponse *response,
                    RPCClosure *done,
                    const turing::QrsServiceSnapshotPtr &snapshot)
    {
        _qrsResponse = response;
        _done = dynamic_cast<multi_call::GigClosure *>(done);
        assert(_done);
        _snapshot = snapshot;
        if (request->has_timeout()) {
            _timeout = request->timeout() * 1000;
        }
        if (request->has_assemblyquery()) {
            _queryStr = request->assemblyquery();
        }
        if (request->has_bizname()) {
            _bizName = request->bizname();
			if (!_bizName.empty()) {
				_useFirstBiz = false;
			}
        }
        _protocolType = request->protocoltype();
    }

    void endQrsSession(QrsSessionSearchResult &sessionSearchResult);
    void setClientAddress(const std::string &clientAddress) {
        _clientAddress = clientAddress;
    }

    void reset();

    void setDropped();

    void setPoolCache(const suez::turing::PoolCachePtr &poolCache) {
        _poolCache = poolCache;
    }
    void setRunIdAllocator(suez::turing::RunIdAllocatorPtr runIdAllocator) {
        _runIdAllocator = runIdAllocator;
    }
protected:
    void reportMetrics(kmonitor::MetricsReporter *metricsReporter);
    bool isTimeout(int64_t currentTime);
private:
    void recycleRunGraphResource();
    void fillResponse(const std::string &result,
                      HaCompressType compressType,
                      ResultFormatType formatType,
                      proto::QrsResponse *response);
    std::string getBizName(const std::string &bizName);
private:
    inline proto::CompressType convertCompressType(HaCompressType compressType);
    inline proto::FormatType convertFormatType(ResultFormatType formatType);
    bool initTimeoutTerminator(int64_t currentTime);
protected:
    turing::QrsServiceSnapshotPtr _snapshot;
    proto::QrsResponse *_qrsResponse = nullptr;
    multi_call::GigClosure *_done = nullptr;
    // query input
    std::string _queryStr;
    std::string _clientAddress;
    std::string _bizName;
    int64_t _timeout = -1;
    common::TimeoutTerminator *_timeoutTerminator = nullptr;
    suez::turing::PoolCachePtr _poolCache;
    suez::turing::RunIdAllocatorPtr _runIdAllocator;
    int64_t _runId = -1;
    autil::mem_pool::Pool *_pool = nullptr;
    proto::ProtocolType _protocolType;
	bool _useFirstBiz = true;
private:
    turing::QrsRunGraphContextPtr _runGraphContext;
private:
    friend class QrsArpcSessionTest;

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsArpcSession);

inline proto::FormatType QrsArpcSession::convertFormatType(ResultFormatType formatType) {
    switch (formatType)
    {
    case RF_PROTOBUF:
        return proto::FT_PROTOBUF;
        break;
    case RF_FB_SUMMARY:
        return proto::FT_FB_SUMMARY;
        break;
    default:
        return proto::FT_XML;
        break;
    }
}

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSARPCSESSION_H
