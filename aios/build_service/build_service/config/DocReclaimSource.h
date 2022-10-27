#ifndef ISEARCH_BS_DOCRECLAIMSOURCE_H
#define ISEARCH_BS_DOCRECLAIMSOURCE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class DocReclaimSource : public autil::legacy::Jsonizable
{
public:
    DocReclaimSource();
    ~DocReclaimSource();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    
public:
    std::string swiftRoot;
    std::string topicName;
    std::string clientConfigStr;
    std::string readerConfigStr;
    int64_t msgTTLInSeconds;
    
public:
    static std::string DOC_RECLAIM_SOURCE;
    
private:
    BS_LOG_DECLARE();

};

BS_TYPEDEF_PTR(DocReclaimSource);

}
}

#endif //ISEARCH_BS_DOCRECLAIMSOURCE_H
