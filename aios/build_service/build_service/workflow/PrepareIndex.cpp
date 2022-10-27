#include <indexlib/document/raw_document/default_raw_document.h>
#include "build_service/workflow/PrepareIndex.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/TestBrokerFactory.h"
#include "build_service/document/RawDocument.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/util/FileUtil.h"
#include "build_service/document/RawDocumentHashMapManager.h"

using namespace std;
using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::document;

namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, PrepareIndex);

document::RawDocumentHashMapManagerPtr PrepareIndex::_hashMapManager(
        new document::RawDocumentHashMapManager());

PrepareIndex::PrepareIndex(const string &outputDir,
                           const std::string &configPath,
                           const std::string &clusterName,
                           int32_t generationId,
                           uint16_t from, uint16_t to,
                           const std::string &dataTable)
    : _outputDir(outputDir)
    , _configPath(configPath)
    , _clusterName(clusterName)
    , _dataTable(dataTable)
    , _generationId(generationId)
    , _from(from)
    , _to(to)
{
    FileUtil::mkDir(_outputDir, true);
}

PrepareIndex::~PrepareIndex() {
}

bool PrepareIndex::buildDocuments(const vector<map<string, string> > &docs) {
    vector<RawDocumentPtr> docVec;
    for (size_t i = 0; i < docs.size(); ++i) {
        docVec.push_back(prepareDoc(docs[i]));
    }
    return buildDocuments(docVec);
}

bool PrepareIndex::buildDocuments(const vector<tr1::shared_ptr<document::RawDocument> > &docs) {
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = _outputDir;
    PartitionId pid;
    *pid.add_clusternames() = _clusterName;
    pid.mutable_range()->set_from(_from);
    pid.mutable_range()->set_to(_to);
    pid.mutable_buildid()->set_generationid(_generationId);
    pid.mutable_buildid()->set_datatable(_dataTable);
    TestBrokerFactory brokerFactory(docs);
    BuildFlow::BuildFlowMode buildFlowMode = BuildFlow::PROCESSOR_AND_BUILDER;
    WorkflowMode mode = JOB;
    BuildFlow buildFlow;
    if (!buildFlow.startBuildFlow(resourceReader, kvMap, pid,
                                  &brokerFactory, buildFlowMode, mode, NULL)) {
        BS_LOG(INFO, "start failed!");
        return false;
    }
    return buildFlow.waitFinish();
}

RawDocumentPtr PrepareIndex::prepareDoc(const map<string, string> &doc) {
    RawDocumentPtr pRawDoc(new IE_NAMESPACE(document)::DefaultRawDocument(_hashMapManager));
    for (map<string, string>::const_iterator it = doc.begin();
         it != doc.end(); ++it)
    {
        pRawDoc->setField(it->first, it->second);
    }
    return pRawDoc;
}

bool PrepareIndex::buildOneDocument() {
    map<string, string> doc;
    doc["CMD"] = "add";
    doc["id"] = "1";
    doc["subject"] = "subject";
    doc["summary"] = "summary";
    doc["city"] = "city";;
    doc["company_id"] = "1";
    return buildOneDocument(doc);
}

bool PrepareIndex::buildOneDocument(const map<string, string> &doc) {
    vector<map<string, string> > docs;
    docs.push_back(doc);
    return buildDocuments(docs);
}

bool PrepareIndex::makeEmptyVersion() {
    vector<RawDocumentPtr> docVec;
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(_configPath);
    KeyValueMap kvMap;
    kvMap[INDEX_ROOT_PATH] = _outputDir;
    PartitionId pid;
    *pid.add_clusternames() = _clusterName;
    pid.mutable_range()->set_from(_from);
    pid.mutable_range()->set_to(_to);
    pid.mutable_buildid()->set_generationid(_generationId);
    pid.mutable_buildid()->set_datatable(_dataTable);
    TestBrokerFactory brokerFactory(docVec);
    BuildFlow::BuildFlowMode buildFlowMode = BuildFlow::PROCESSOR_AND_BUILDER;
    WorkflowMode mode = JOB;
    BuildFlow buildFlow;
    if (!buildFlow.startBuildFlow(resourceReader, kvMap, pid,
                                  &brokerFactory, buildFlowMode, mode, NULL)) {
        BS_LOG(INFO, "start failed!");
        return false;
    }
    if (!buildFlow.waitFinish()) {
        BS_LOG(INFO, "fatal error happened!");
        return false;
    }

    return true;
}

}
}
