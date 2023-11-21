#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "build_service/proto/BasicDefs.pb.h"

namespace build_service { namespace admin {

typedef std::vector<std::pair<proto::BuildId, std::string>> OperationQueue;

void setDelegateToFake(bool delegate);
void setRecoverFail(const proto::BuildId& buildId);
void setRecoverSucc(const proto::BuildId& buildId);
OperationQueue& getOperationQueue();

}} // namespace build_service::admin
