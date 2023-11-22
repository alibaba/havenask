#include "build_service/admin/test/FakeSwiftAdminAdapter.h"

#include <iosfwd>

#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/Log.h"
#include "fslib/util/FileUtil.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"

using namespace std;
using namespace autil;
using namespace build_service::util;

using namespace swift::protocol;
using namespace swift::network;

namespace swift { namespace client {
AUTIL_LOG_SETUP(client, FakeSwiftAdminAdapter);

FakeSwiftAdminAdapter::FakeSwiftAdminAdapter(const std::string& path)
    : SwiftAdminAdapter(path, SwiftRpcChannelManagerPtr())
    , _ec(ERROR_NONE)
    , _topicZkPath(path)
{
}

FakeSwiftAdminAdapter::~FakeSwiftAdminAdapter() {}

ErrorCode FakeSwiftAdminAdapter::deleteTopicBatch(TopicBatchDeletionRequest& request,
                                                  TopicBatchDeletionResponse& response)
{
    vector<string> deleteTopics;
    deleteTopics.reserve(request.topicnames_size());
    for (int i = 0; i < request.topicnames_size(); i++) {
        string topicName = request.topicnames(i);
        deleteTopics.push_back(topicName);
    }
    _latestDeleteTopics.swap(deleteTopics);

    if (_ec == ERROR_NONE) {
        for (int i = 0; i < request.topicnames_size(); i++) {
            string topicName = request.topicnames(i);
            BS_LOG(INFO, "delete swift topic[%s] done", topicName.c_str());
            string filename = fslib::util::FileUtil::joinFilePath(_topicZkPath, topicName);
            bool exist = false;
            fslib::util::FileUtil::isExist(filename, exist);
            if (!exist) {
                continue;
            }
            fslib::util::FileUtil::remove(filename);
        }
    }

    protocol::ErrorInfo* ei = response.mutable_errorinfo();
    ei->set_errcode(_ec);
    string errMsg = ErrorCode_Name(ei->errcode());
    if (_errorMsg.size() > 0) {
        errMsg += "[" + _errorMsg + "]";
    }
    ei->set_errmsg(errMsg);
    return _ec;
}

ErrorCode FakeSwiftAdminAdapter::createTopicBatch(TopicBatchCreationRequest& request,
                                                  TopicBatchCreationResponse& response)
{
    vector<string> createTopics;
    createTopics.reserve(request.topicrequests_size());
    for (int i = 0; i < request.topicrequests_size(); i++) {
        string topicName = request.topicrequests(i).topicname();
        createTopics.push_back(topicName);
    }
    _latestCreateTopics.swap(createTopics);

    if (_ec == ERROR_NONE) {
        for (int i = 0; i < request.topicrequests_size(); i++) {
            string topicName = request.topicrequests(i).topicname();
            BS_LOG(INFO, "add swift topic[%s] done", topicName.c_str());
            string filename = fslib::util::FileUtil::joinFilePath(_topicZkPath, topicName);
            bool exist = false;
            fslib::util::FileUtil::isExist(filename, exist);
            if (exist) {
                continue;
            }
            fslib::util::FileUtil::writeFile(filename, "topic");
        }
    }

    if (_ec == ERROR_ADMIN_TOPIC_HAS_EXISTED) {
        vector<string> topics = StringUtil::split(_errorMsg, ";");
        for (auto& topicName : topics) {
            BS_LOG(INFO, "swift topic[%s] already exists", topicName.c_str());
            string filename = fslib::util::FileUtil::joinFilePath(_topicZkPath, topicName);
            bool exist = false;
            fslib::util::FileUtil::isExist(filename, exist);
            if (exist) {
                continue;
            }
            fslib::util::FileUtil::writeFile(filename, "topic");
        }
    }

    protocol::ErrorInfo* ei = response.mutable_errorinfo();
    ei->set_errcode(_ec);
    string errMsg = ErrorCode_Name(ei->errcode());
    if (_errorMsg.size() > 0) {
        errMsg += "[" + _errorMsg + "]";
    }
    ei->set_errmsg(errMsg);
    return _ec;
}

}} // namespace swift::client
