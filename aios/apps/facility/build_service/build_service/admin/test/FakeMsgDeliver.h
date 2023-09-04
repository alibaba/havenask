#ifndef ISEARCH_BS_FAKEMSGDELIVERER_H
#define ISEARCH_BS_FAKEMSGDELIVERER_H

#include "autil/legacy/any.h"
#include "build_service/admin/MsgDeliverer.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/test/test.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {
class SingleBuilderTask;

class FakeMsgDeliverer : public MsgDeliverer
{
public:
    FakeMsgDeliverer() {}
    ~FakeMsgDeliverer() {}

public:
    bool getMsg(const std::string& name, const std::string& msgName, autil::legacy::Any& anyMsg)
    {
        if (name == "builder") {
            proto::BuildStep* buildStep = autil::legacy::AnyCast<proto::BuildStep>(&anyMsg);
            if (buildStep == NULL) {
                assert(false);
                return false;
            }
            *buildStep = _buildStep;
            return true;
        }
        return MsgDeliverer::getMsg(name, msgName, anyMsg);
    }

private:
    proto::BuildStep _buildStep;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeMsgDeliverer);

}} // namespace build_service::admin

#endif // ISEARCH_BS_FAKEMSGDELIVERER_H
