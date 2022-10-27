#ifndef ISEARCH_MOCKCAVAJITWRAPPER_H
#define ISEARCH_MOCKCAVAJITWRAPPER_H

#define private public
#include <suez/turing/common/CavaJitWrapper.h>
#include <suez/turing/common/Closure.h>
#include <suez/turing/common/QueryResource.h>

BEGIN_HA3_NAMESPACE(turing);
class MockCavaJitWrapper : public suez::turing::CavaJitWrapper {
public:
    MockCavaJitWrapper(const std::string &configPath,
                       const suez::turing::CavaConfig &cavaConfig)
        : CavaJitWrapper(configPath, cavaConfig, nullptr) {
    }
    MOCK_METHOD6(compileAsync,
                 bool(const std::vector<std::string> &moduleNames,
                      const std::vector<std::string> &fileNames,
                      const std::vector<std::string> &codes,
                      tensorflow::QueryResource *queryResource,
                      suez::turing::ClosurePtr closure,
                      const cava::CavaModuleOptions &options));
};

typedef ::testing::StrictMock<MockCavaJitWrapper> StrictMockCavaJitWrapper;
typedef ::testing::NiceMock<MockCavaJitWrapper> NiceMockCavaJitWrapper;

END_HA3_NAMESPACE();
#endif  // ISEARCH_MOCKCAVAJITWRAPPER_H
