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
#ifndef ISEARCH_BS_READERMANAGER_H
#define ISEARCH_BS_READERMANAGER_H

#include "build_service/reader/ReaderConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {
class RawDocumentReader;
class ReaderModuleFactory;
}} // namespace build_service::reader

namespace build_service { namespace plugin {
class PlugInManager;
}} // namespace build_service::plugin

namespace build_service { namespace config {
class ResourceReader;
}} // namespace build_service::config

namespace build_service { namespace reader {

class ReaderManager
{
public:
    ReaderManager(const std::shared_ptr<config::ResourceReader>& resourceReaderPtr);
    virtual ~ReaderManager();

private:
    ReaderManager(const ReaderManager&);
    ReaderManager& operator=(const ReaderManager&);

public:
    bool init(const ReaderConfig& readerConfig);
    RawDocumentReader* getRawDocumentReaderByModule(const plugin::ModuleInfo& moduleInfo);

public:
protected:
    virtual ReaderModuleFactory* getModuleFactory(const plugin::ModuleInfo& moduleInfo);
    virtual const std::shared_ptr<plugin::PlugInManager> getPlugInManager(const plugin::ModuleInfo& moduleInfo);

private:
    std::shared_ptr<config::ResourceReader> _resourceReaderPtr;
    std::shared_ptr<plugin::PlugInManager> _plugInManagerPtr;
    ReaderConfig _readerConfig;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ReaderManager);

}} // namespace build_service::reader

#endif // ISEARCH_BS_READERMANAGER_H
