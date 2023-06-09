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

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "autil/OptionParser.h"

namespace http_arpc {
class HTTPRPCServer;
}

namespace worker_framework {

class WorkerBaseImpl;

class WorkerBase {
public:
    WorkerBase();
    virtual ~WorkerBase();

private:
    WorkerBase(const WorkerBase &);
    WorkerBase &operator=(const WorkerBase &);

public:
    bool init(int argc, char **argv);
    bool run();
    bool registerService(RPCService *rpcService, arpc::ThreadPoolDescriptor tpd = arpc::ThreadPoolDescriptor());

    autil::OptionParser &getOptionParser();
    const autil::OptionParser &getOptionParser() const;

    std::string getIp() const;
    uint32_t getPort() const;
    void setPort(uint32_t port);
    std::string getAddress() const;

    uint32_t getHTTPPort() const;
    void setHTTPPort(uint32_t httpPort);
    std::string getHTTPAddress() const;
    bool supportHTTP() const;
    bool initHTTPRPCServer();
    bool initHTTPRPCServer(uint32_t threadNum, uint32_t queueSize);

    std::string getCwdPath() const;
    std::string getLogConfFile() const;
    virtual std::string getVersion();
    bool getIsRecommendPort() const;
    void stop();

    // closer server only, you must invoke destoryRpcServer next to delete server
    void closeRpcServer();
    void destoryRpcServer();

    // maybe stop rpc server when your app stop.
    // this func is equal to invoke cloaseRpcServer and destoryRpcServer
    void stopRPCServer();

    http_arpc::HTTPRPCServer *getHTTPRPCServer();

protected:
    virtual bool initMonitor() { return true; }
    virtual void doInitOptions(){};
    virtual void doExtractOptions(){};
    virtual bool doInit() { return true; }
    virtual bool doStart() { return true; }
    virtual bool doPrepareToRun() { return true; }
    virtual void doIdle(){};
    virtual void doStop(){};
    void setReInit();
    arpc::ANetRPCServer *getRPCServer();

private:
    void initOptions();
    void extractOptions();
    bool initLog();
    bool initRPCServer();
    bool startRPCServer();

private:
    void initDeamon();
    int32_t listen(const std::function<bool(std::string)> &listenFunc, int32_t port);
    bool changeCurrentPath(const std::string &path);
    bool isStopped();

    // for test
    void setRPCServer(arpc::ANetRPCServer *rpcServer);
    void setHTTPRPCServer(http_arpc::HTTPRPCServer *httpRPCServer);

private:
    WorkerBaseImpl *_impl;
};

WorkerBase *createWorker();

typedef std::shared_ptr<WorkerBase> WorkerBasePtr;

}; // namespace worker_framework
