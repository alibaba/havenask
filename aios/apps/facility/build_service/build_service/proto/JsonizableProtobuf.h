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

#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/util/Log.h"

namespace build_service { namespace proto {
template <class T>
class JsonizableProtobuf : public autil::legacy::Jsonizable
{
public:
    JsonizableProtobuf() {}
    JsonizableProtobuf(const T& object) : _object(object) {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
    {
        if (json.GetMode() == FROM_JSON) {
            http_arpc::ProtoJsonizer::fromJsonMap(json.GetMap(), &_object);
        } else {
            autil::legacy::json::JsonMap jsonMap;
            http_arpc::ProtoJsonizer::toJsonMap(_object, jsonMap);
            json = autil::legacy::Jsonizable::JsonWrapper(autil::legacy::Any(jsonMap));
        }
    }
    void set(const T& object) { _object = object; }
    T& get() { return _object; }
    const T& get() const { return _object; }
    bool operator==(const JsonizableProtobuf<T>& other) const { return get() == other.get(); }

private:
    T _object;
};

}} // namespace build_service::proto
