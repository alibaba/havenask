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

#include "multi_call/interface/Closure.h"

namespace multi_call {
class Reply;
} // namespace multi_call

namespace isearch {
namespace sql {

class GigQuerySessionCallbackCtx;

class GigQuerySessionClosure : public multi_call::Closure {
public:
    GigQuerySessionClosure(std::shared_ptr<GigQuerySessionCallbackCtx> ctx);
    ~GigQuerySessionClosure();
    GigQuerySessionClosure(const GigQuerySessionClosure &) = delete;
    GigQuerySessionClosure &operator=(const GigQuerySessionClosure &) = delete;

public:
    void Run() override;
    std::shared_ptr<multi_call::Reply> &getReplyPtr();

private:
    void doRun();

private:
    std::shared_ptr<multi_call::Reply> _reply;
    std::shared_ptr<GigQuerySessionCallbackCtx> _ctx;
};

} // namespace sql
} // namespace isearch
