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
#include "indexlib/document/document_factory_wrapper.h"

#include "indexlib/document/built_in_document_factory.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::plugin;

namespace indexlib { namespace document {

IE_LOG_SETUP(document, DocumentFactoryWrapper);

DocumentFactoryWrapper::~DocumentFactoryWrapper() {}

DocumentFactory* DocumentFactoryWrapper::CreateBuiltInFactory() const { return new BuiltInDocumentFactory(); }

DocumentFactoryWrapperPtr DocumentFactoryWrapper::CreateDocumentFactoryWrapper(const IndexPartitionSchemaPtr& schema,
                                                                               const string& identifier,
                                                                               const string& pluginPath)
{
    DocumentFactoryWrapperPtr wrapper(new DocumentFactoryWrapper(schema));
    CustomizedConfigPtr docParserConfig;
    if (schema) {
        docParserConfig =
            CustomizedConfigHelper::FindCustomizedConfig(schema->GetCustomizedDocumentConfigs(), identifier);
    }
    if (!docParserConfig) {
        IE_LOG(INFO, "Init builtin DocumentFactoryWrapper");
        if (!wrapper->Init()) {
            IE_LOG(ERROR, "Init builtin DocumentFactoryWrapper fail!");
            return DocumentFactoryWrapperPtr();
        }
        return wrapper;
    }

    string pluginName = docParserConfig->GetPluginName();
    if (pluginName.empty()) {
        IE_LOG(ERROR,
               "pluginName should not be empty in "
               "customized_document_configs for table [%s]!",
               schema->GetSchemaName().c_str());
        return DocumentFactoryWrapperPtr();
    }
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = pluginName;
    moduleInfo.modulePath = PluginManager::GetPluginFileName(pluginName);

    IE_LOG(INFO,
           "Init plugin DocumentFactoryWrapper, "
           "pluginName[%s], tableName[%s], pluginPath[%s]",
           pluginName.c_str(), schema->GetSchemaName().c_str(), pluginPath.c_str());
    if (!wrapper->Init(pluginPath, moduleInfo)) {
        IE_LOG(ERROR,
               "DocumentProcessorChain init plugin DocumentFactoryWrapper fail, "
               "pluginName[%s], tableName[%s], pluginPath[%s]",
               pluginName.c_str(), schema->GetSchemaName().c_str(), pluginPath.c_str());
        return DocumentFactoryWrapperPtr();
    }
    return wrapper;
}
}} // namespace indexlib::document
