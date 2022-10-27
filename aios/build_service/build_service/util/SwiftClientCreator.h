#ifndef ISEARCH_BS_SWIFTCLIENTCREATOR_H
#define ISEARCH_BS_SWIFTCLIENTCREATOR_H

#include "swift/client/SwiftClient.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/ErrorCollector.h"

namespace build_service {
namespace util {

class SwiftClientCreator : public proto::ErrorCollector
{
public:
    SwiftClientCreator();
    virtual ~SwiftClientCreator();

private:
    SwiftClientCreator(const SwiftClientCreator &);
    SwiftClientCreator& operator=(const SwiftClientCreator &);

public:
    SWIFT_NS(client)::SwiftClient * createSwiftClient(const std::string &zfsRootPath,
            const std::string& swiftClientConfig);

private:
    virtual SWIFT_NS(client)::SwiftClient * doCreateSwiftClient(const std::string &zfsRootPath,
            const std::string& swiftClientConfig);
    static std::string getInitConfigStr(const std::string &zfsRootPath, const std::string& swiftClientConfig);

private:
    mutable autil::ThreadMutex _lock;
    std::map<std::string, SWIFT_NS(client)::SwiftClient*> _swiftClientMap;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftClientCreator);

}
}

#endif //ISEARCH_BS_SWIFTCLIENTCREATOR_H
