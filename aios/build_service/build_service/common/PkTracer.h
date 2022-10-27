#ifndef ISEARCH_BS_PKTRACER_H
#define ISEARCH_BS_PKTRACER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace common {

class PkTracer
{
private:
    PkTracer();
    ~PkTracer();
private:
    PkTracer(const PkTracer &);
    PkTracer& operator=(const PkTracer &);
public:
    static void fromReadTrace(const std::string &str, int64_t locatorOffset);
    static void toSwiftTrace(const std::string &str, const std::string& locator);
    static void fromSwiftTrace(const std::string &str, const std::string& locator, int64_t brokerMsgId);
    static void toBuildTrace(const std::string &str, int originDocOperator, int docOperator);
    static void buildFailTrace(const std::string &str, int originDocOperator, int docOperator);
private:
    BS_LOG_DECLARE();
};


}
}

#endif //ISEARCH_BS_PKTRACER_H
