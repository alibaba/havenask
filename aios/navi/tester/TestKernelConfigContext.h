#pragma once

#include "autil/legacy/jsonizable.h"
#include "navi/engine/KernelConfigContext.h"

namespace navi {

struct KernelTestInfo;

class TestKernelConfigContext : public KernelConfigContext {
public:
    TestKernelConfigContext(const std::string &configPath);
    ~TestKernelConfigContext();
    bool init(const KernelTestInfo &info);
public:
    static KernelConfigContextPtr buildConfigContext(
            const KernelTestInfo &info);
private:
    void initBinaryAttrs(const std::map<std::string, std::string> &binaryAttrs);
    void initIntegerAttrs(const std::map<std::string, int64_t> &integerAttrs);
    NodeDef *getNodeDef();
private:
    std::string _configPath;
    autil::legacy::RapidDocument _jsonAttrsDocument;
    autil::legacy::RapidDocument _jsonConfigDocument;
    NodeDef *_testNodeDef;
};

NAVI_TYPEDEF_PTR(TestKernelConfigContext);

}

