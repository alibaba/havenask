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

#include "navi/engine/Resource.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"

namespace navi {
class GigClientResource;
} // namespace navi

namespace isearch {
namespace sql {

class MessageWriterManager;
class SqlConfigResource;

class MessageWriterManagerR : public navi::Resource
{
public:
    MessageWriterManagerR();
    ~MessageWriterManagerR();
    MessageWriterManagerR(const MessageWriterManagerR &) = delete;
    MessageWriterManagerR &operator=(const MessageWriterManagerR &) = delete;
public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
public:
    MessageWriterManager *getMessageWriterManager() const;
private:
    bool initMessageWriterManger();
public:
    static const std::string RESOURCE_ID;
public:
    std::unique_ptr<isearch::sql::MessageWriterManager> _messageWriterManager;
    navi::GigClientResource *_gigClientResource = nullptr;
    SqlConfigResource *_sqlConfigResource = nullptr;
};

NAVI_TYPEDEF_PTR(MessageWriterManagerR);

}
}
