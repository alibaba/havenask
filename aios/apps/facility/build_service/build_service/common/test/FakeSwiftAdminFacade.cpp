#include "build_service/common/test/FakeSwiftAdminFacade.h"

#include <iosfwd>
#include <map>

#include "alog/Logger.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::config;

namespace build_service { namespace common {

BS_LOG_SETUP(common, FakeSwiftAdminFacade);

FakeSwiftAdminFacade::~FakeSwiftAdminFacade() {}

bool FakeSwiftAdminFacade::createTopic(const swift::network::SwiftAdminAdapterPtr& adapter, const string& topicName,
                                       const config::SwiftTopicConfig& config, bool needVersionControl)
{
    if (!_createTopicSuccess) {
        return false;
    }
    KeyValueMap param;
    param["need_version_control"] = needVersionControl ? "true" : "false";
    BS_LOG(INFO, "add swift topic[%s] done", topicName.c_str());
    string filename = fslib::util::FileUtil::joinFilePath(_brokerTopicZkPath, topicName);
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(filename, exist)) {
        return false;
    }
    if (exist) {
        return true;
    }
    return fslib::util::FileUtil::writeFile(filename, autil::legacy::ToJsonString(param));
}

bool FakeSwiftAdminFacade::deleteTopic(const swift::network::SwiftAdminAdapterPtr& adapter, const string& topicName)
{
    if (!_deleteTopicSuccess) {
        return false;
    }
    BS_LOG(INFO, "delete swift topic[%s] done", topicName.c_str());
    string filename = fslib::util::FileUtil::joinFilePath(_brokerTopicZkPath, topicName);
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(filename, exist)) {
        return false;
    }
    if (!exist) {
        return true;
    }
    return fslib::util::FileUtil::remove(filename);
}

}} // namespace build_service::common
