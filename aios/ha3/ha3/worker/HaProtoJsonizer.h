#ifndef ISEARCH_HAPROTOJSONIZER_H
#define ISEARCH_HAPROTOJSONIZER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <http_arpc/ProtoJsonizer.h>

BEGIN_HA3_NAMESPACE(worker);

class HaProtoJsonizer: public http_arpc::ProtoJsonizer
{
public:
    HaProtoJsonizer();
    ~HaProtoJsonizer();
private:
    HaProtoJsonizer(const HaProtoJsonizer &);
    HaProtoJsonizer& operator=(const HaProtoJsonizer &);
public:
    bool fromJson(const std::string &jsonStr, 
                  ::google::protobuf::Message *message) override;
    std::string toJson(const ::google::protobuf::Message &message) override;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<HaProtoJsonizer> HaProtoJsonizerPtr;

END_HA3_NAMESPACE(worker);

#endif //ISEARCH_HAPROTOJSONIZER_H
