#include "build_service/admin/test/FakeAdminTaskBase.h"

using namespace std;
using namespace build_service::proto;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FakeAdminTaskBase);

FakeAdminTaskBase::FakeAdminTaskBase(const BuildId& buildId, const TaskResourceManagerPtr& resMgr)
    : AdminTaskBase(buildId, resMgr)
{
}

FakeAdminTaskBase::~FakeAdminTaskBase() {}

}} // namespace build_service::admin
