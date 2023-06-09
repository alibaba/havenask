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
#ifndef HTTP_ARPC_PROTOJSONIZER_H
#define HTTP_ARPC_PROTOJSONIZER_H

#include "google/protobuf/message.h"
#include "autil/legacy/jsonizable.h"
#include <memory>
namespace http_arpc {

class ProtoJsonizer
{
public:
    static const std::string ORIGINAL_STRING;
    static const std::string HTTP_STATUS_CODE;
public:
    ProtoJsonizer();
    virtual ~ProtoJsonizer();
private:
    ProtoJsonizer(const ProtoJsonizer &);
    ProtoJsonizer& operator=(const ProtoJsonizer &);
public:
    static bool fromJsonString(const std::string &jsonStr, ::google::protobuf::Message *message);
    static std::string toJsonString(const ::google::protobuf::Message &message,
                                    bool outputDefault=false);
    static void fromJsonMap(const autil::legacy::json::JsonMap &jsonMap, ::google::protobuf::Message *message);
    static void toJsonMap(const ::google::protobuf::Message &message,
                          autil::legacy::json::JsonMap &jsonMap,
                          bool outputDefault=false);

    static std::string toJsonString(const ::google::protobuf::Message &message,
                                    const std::string &responseField,
                                    bool outputDefault=false);
public:
    virtual bool fromJson(const std::string &jsonStr, ::google::protobuf::Message *message);
    virtual std::string toJson(const ::google::protobuf::Message &message);
    virtual int getStatusCode(const google::protobuf::Message& message);
public:
    void setResponseField(const std::string &responseField);
protected:
    static void setOneValue(const ::google::protobuf::FieldDescriptor* fieldDesc, 
                            const autil::legacy::Any &any,
                            ::google::protobuf::Message *message);
    template <typename T>
    static T parseType(const autil::legacy::Any &any);
private:
    std::string _responseField;
};

typedef std::shared_ptr<ProtoJsonizer> ProtoJsonizerPtr;
}
#endif //HTTP_ARPC_PROTOJSONIZER_H
