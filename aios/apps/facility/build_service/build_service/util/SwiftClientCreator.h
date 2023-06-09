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
#ifndef ISEARCH_BS_SWIFTCLIENTCREATOR_H
#define ISEARCH_BS_SWIFTCLIENTCREATOR_H

#include "build_service/common_define.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/util/Log.h"
#include "swift/client/SwiftClient.h"

namespace build_service { namespace util {

class SwiftClientCreator : public proto::ErrorCollector
{
public:
    SwiftClientCreator();
    SwiftClientCreator(const kmonitor::MetricsReporterPtr& metricsReporter);
    virtual ~SwiftClientCreator();

private:
    SwiftClientCreator(const SwiftClientCreator&);
    SwiftClientCreator& operator=(const SwiftClientCreator&);

public:
    swift::client::SwiftClient* createSwiftClient(const std::string& zfsRootPath, const std::string& swiftClientConfig);

private:
    virtual swift::client::SwiftClient* doCreateSwiftClient(const std::string& zfsRootPath,
                                                            const std::string& swiftClientConfig);
    static std::string getInitConfigStr(const std::string& zfsRootPath, const std::string& swiftClientConfig);

private:
    mutable autil::ThreadMutex _lock;
    std::map<std::string, swift::client::SwiftClient*> _swiftClientMap;
    kmonitor::MetricsReporterPtr _metricsReporter = nullptr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftClientCreator);

}} // namespace build_service::util

#endif // ISEARCH_BS_SWIFTCLIENTCREATOR_H
