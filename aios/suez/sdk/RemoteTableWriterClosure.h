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

#include "autil/result/Result.h"
#include "multi_call/interface/Closure.h"
#include "suez/sdk/RemoteTableWriterParam.h"

namespace multi_call {
class Response;
class Reply;
} // namespace multi_call

namespace suez {

class RemoteTableWriterClosure : public multi_call::Closure {
public:
    RemoteTableWriterClosure(std::function<void(autil::Result<WriteCallbackParam>)> cb);
    ~RemoteTableWriterClosure();

public:
    virtual void Run() override;
    std::shared_ptr<multi_call::Reply> &getReplyPtr();

private:
    void doRun();
    bool getDataFromResponse(const std::shared_ptr<multi_call::Response> &response);

private:
    std::shared_ptr<multi_call::Reply> _reply;
    std::function<void(autil::result::Result<WriteCallbackParam>)> _cb;
    WriteCallbackParam _cbParam;
};

} // namespace suez
