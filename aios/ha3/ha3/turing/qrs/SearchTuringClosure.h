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

#include <stdint.h>
#include <memory>

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "suez/turing/proto/Search.pb.h"

#include "ha3/common/AccessLog.h" // IWYU pragma: keep
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace proto {
class QrsResponse;
}  // namespace proto
}  // namespace isearch

namespace isearch {
namespace turing {

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
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch

