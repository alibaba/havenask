#ifndef NAVI_KERNELTESTER_H
#define NAVI_KERNELTESTER_H

#include "navi/engine/Navi.h"
#include "navi/engine/Kernel.h"
#include <thread>

namespace navi {

struct KernelTestInfo {
public:
    KernelTestInfo()
        : resources(new ResourceMap())
        , logLevel("warn")
        , threadNum(1)
        , disablePerf(true)
        , disableSymbolTable(true)
    {
    }
public:
    std::string kernelName;
    std::set<std::string> inputs;
    std::set<std::string> outputs;
    std::string configPath;
    std::string configs;
    std::string attrs;
    std::map<std::string, std::string> binaryAttrs;
    std::map<std::string, int64_t> integerAttrs;
    bool skipConfig = false;
    bool skipInit = false;
    bool skipDeleteKernel = true;
    ResourceMapPtr resources;
    std::map<std::string, std::string> resourceConfigs;
    std::map<std::string, std::string> snapshotResourceConfig;
    std::vector<std::string> modulePaths;
    std::string logLevel;
    size_t threadNum;
    std::vector<std::string> kernelNameRegexVec;
    bool disablePerf;
    bool disableSymbolTable;
    std::vector<NamedData> namedDataVec;
};

class KernelTesterBuilder;
class NaviTesterResource;

class KernelTester
{
public:
    KernelTester(const std::string &logPrefix, const NaviPtr &navi,
                 const KernelTestInfo &info);
    ~KernelTester();
private:
    KernelTester(const KernelTester &);
    KernelTester &operator=(const KernelTester &);
public:
    bool setInput(const std::string &portName, const DataPtr &data);
    bool setInputEof(const std::string &portName);
    bool setInput(const std::string &portName, const DataPtr &data, bool eof);
    bool compute();
    bool getOutput(const std::string &portName, DataPtr &data, bool &eof);
    bool isFinished() const;
    Kernel *getKernel() const;
    KernelConfigContextPtr getConfigContext() const;
    KernelInitContextPtr getInitContext() const;
    KernelComputeContextPtr getComputeContext() const;
    ErrorCode getErrorCode() const;
    bool hasError() const;
    std::string getErrorMessage() const;
    LoggingEventPtr getErrorEvent() const;
    Node *getNode() const;
private:
    bool init();
    GraphDef *buildTestGraph();
    bool runGraph(GraphDef *graphDef);
    ErrorCode computeKernel();
    bool checkFail() const;
    friend class KernelTesterBuilder;
    friend std::ostream &operator<<(std::ostream &os,
                                    const KernelTester &tester);
private:
    DECLARE_LOGGER();
    std::thread::id _threadId;
    NaviPtr _navi;
    KernelTestInfo _info;
    NaviUserResultPtr _result;
    std::shared_ptr<NaviTesterResource> _testerResource;
};

NAVI_TYPEDEF_PTR(KernelTester);

extern std::ostream &operator<<(std::ostream &os, const KernelTester &tester);

}

#endif //NAVI_KERNELTESTER_H
