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

#include <memory>
#include <string>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/resource/GigClientR.h"
#include "sql/config/SwiftWriteConfig.h"
#include "sql/config/TableWriteConfig.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class MessageWriterManager;

class MessageWriterManagerR : public navi::Resource {
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

private:
    RESOURCE_DEPEND_DECLARE();

public:
    std::unique_ptr<MessageWriterManager> _messageWriterManager;
    RESOURCE_DEPEND_ON(navi::GigClientR, _gigClientR);
    sql::SwiftWriteConfig _swiftWriterConfig;
    sql::TableWriteConfig _tableWriterConfig;
};

NAVI_TYPEDEF_PTR(MessageWriterManagerR);

} // namespace sql
