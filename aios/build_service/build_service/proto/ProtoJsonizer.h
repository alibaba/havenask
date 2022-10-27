#ifndef ISEARCH_BS_PROTOJSONIZER_H
#define ISEARCH_BS_PROTOJSONIZER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <google/protobuf/message.h>
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace proto {

class ProtoJsonizer
{
private:
    ProtoJsonizer();
    ~ProtoJsonizer();
    ProtoJsonizer(const ProtoJsonizer &);
    ProtoJsonizer& operator=(const ProtoJsonizer &);
public:
    static bool fromJsonString(const std::string &jsonStr, ::google::protobuf::Message *message);
    static std::string toJsonString(const ::google::protobuf::Message &message);
private:
    static void doFromJsonString(const autil::legacy::json::JsonMap &jsonMap,
                                ::google::protobuf::Message *message);
    static void doToJsonString(const ::google::protobuf::Message &message,
                               autil::legacy::json::JsonMap &jsonMap);
    static void setOneValue(const ::google::protobuf::FieldDescriptor* fieldDesc, 
                            const autil::legacy::Any &any,
                            ::google::protobuf::Message *message);
    template <typename T>
    static T parseType(const autil::legacy::Any &any);
private:
    BS_LOG_DECLARE();
};

}
}
#endif //ISEARCH_BS_PROTOJSONIZER_H
