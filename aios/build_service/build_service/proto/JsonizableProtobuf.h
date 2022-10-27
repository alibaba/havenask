#ifndef ISEARCH_BS_JSONIZABLEPROTOBUF_H
#define ISEARCH_BS_JSONIZABLEPROTOBUF_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <http_arpc/ProtoJsonizer.h>
#include <autil/legacy/jsonizable.h>
#include "build_service/proto/ProtoComparator.h"

namespace build_service {
namespace proto {
template <class T>
class JsonizableProtobuf : public autil::legacy::Jsonizable {
public:
    JsonizableProtobuf() {}
    JsonizableProtobuf(const T &object) :_object(object) {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        if (json.GetMode() == FROM_JSON) {
            http_arpc::ProtoJsonizer::fromJsonMap(json.GetMap(), &_object);
        } else {
            autil::legacy::json::JsonMap jsonMap;
            http_arpc::ProtoJsonizer::toJsonMap(_object, jsonMap);
            json = autil::legacy::Jsonizable::JsonWrapper(autil::legacy::Any(jsonMap));
        }
    }
    void set(const T &object) { _object = object; }
    T &get() { return _object; }
    const T &get() const { return _object; }
    bool operator ==(const JsonizableProtobuf<T> &other) const { return get() == other.get(); }
private:
    T _object;
};

}
}

#endif //ISEARCH_BS_JSONIZABLEPROTOBUF_H
