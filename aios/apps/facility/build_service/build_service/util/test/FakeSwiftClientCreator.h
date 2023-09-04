#ifndef ISEARCH_BS_FAKESWIFTCLIENTCREATOR_H
#define ISEARCH_BS_FAKESWIFTCLIENTCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftClientCreator.h"

namespace build_service { namespace util {

// FakeSwiftClientCreator for bs ut
class FakeSwiftClientCreator : public SwiftClientCreator
{
public:
    FakeSwiftClientCreator(bool hasReader, bool hasWriter);
    virtual ~FakeSwiftClientCreator();

private:
    FakeSwiftClientCreator(const FakeSwiftClientCreator&);
    FakeSwiftClientCreator& operator=(const FakeSwiftClientCreator&);

public:
private:
    virtual swift::client::SwiftClient* doCreateSwiftClient(const std::string& zfsRootPath,
                                                            const std::string& swiftClientConfoig) override;
    std::string getInitConfigStr(const std::string& zfsRootPath, const std::string& swiftClientConfig);

private:
    bool _hasReader;
    bool _hasWriter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeSwiftClientCreator);

}} // namespace build_service::util

#endif // ISEARCH_BS_FAKESWIFTCLIENTCREATOR_H
