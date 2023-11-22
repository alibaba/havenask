#pragma once

#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/WorkerNode.h"

namespace build_service { namespace admin {

class FakeAdminTaskBase : public AdminTaskBase
{
public:
    FakeAdminTaskBase(const proto::BuildId& buildId, const TaskResourceManagerPtr& resMgr);
    ~FakeAdminTaskBase();

private:
    FakeAdminTaskBase(const FakeAdminTaskBase&);
    FakeAdminTaskBase& operator=(const FakeAdminTaskBase&);

public:
    virtual bool finish(const KeyValueMap& kvMap) override { return false; }
    virtual void waitSuspended(proto::WorkerNodes& workerNodes) override {}
    virtual void Jsonize(JsonWrapper& json) override {}

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeAdminTaskBase);

}} // namespace build_service::admin
