#ifndef ISEARCH_TURING_QRS_TURING_SEARCH_TURING_CLOSURE_H
#define ISEARCH_TURING_QRS_TURING_SEARCH_TURING_CLOSURE_H

#include <memory>
#include <ha3/common.h>
#include <ha3/common/AccessLog.h>
#include <ha3/util/Log.h>
#include <ha3/proto/QrsService.pb.h>
#include <suez/turing/proto/Search.pb.h>
#include <multi_call/rpc/GigClosure.h>

BEGIN_HA3_NAMESPACE(turing);

class SearchTuringClosure : public multi_call::GigClosure
{
public:
    SearchTuringClosure(
            common::AccessLog* pAccessLog,
            suez::turing::GraphRequest* pGraphRequest,
            suez::turing::GraphResponse* pGraphResponse,
            proto::QrsResponse* pQrsResponse,
            multi_call::GigClosure* pDone,
            int64_t startTime);

    void Run() override;

    int64_t getStartTime() const override {
	return _pDone->getStartTime();
    }
    multi_call::ProtocolType getProtocolType() override {
	return _pDone->getProtocolType();
    }

private:
    std::unique_ptr<common::AccessLog> _pAccessLog;
    std::unique_ptr<suez::turing::GraphRequest> _pGraphRequest;
    std::unique_ptr<suez::turing::GraphResponse> _pGraphResponse;
    proto::QrsResponse *_pQrsResponse{nullptr};
    multi_call::GigClosure *_pDone{ nullptr };
    int64_t _startTime{ 0 };

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_TURING_QRS_TURING_SEARCH_TURING_CLOSURE_H
