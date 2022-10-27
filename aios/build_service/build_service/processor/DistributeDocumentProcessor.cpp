#include <autil/StringTokenizer.h>
#include "build_service/processor/DistributeDocumentProcessor.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;
namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, DistributeDocumentProcessor);

const string DistributeDocumentProcessor::PROCESSOR_NAME = "DistributeDocumentProcessor";
const string DistributeDocumentProcessor::DISTRIBUTE_FIELD_NAMES = "distribute_field_name";
const string DistributeDocumentProcessor::DISTRIBUTE_RULE = "distribute_rule";
const string DistributeDocumentProcessor::DEFAULT_CLUSTERS = "default_clusters";
const string DistributeDocumentProcessor::OTHER_CLUSTERS = "other_clusters";
const string DistributeDocumentProcessor::EMPTY_CLUSTERS = "empty_clusters";
const string DistributeDocumentProcessor::ALL_CLUSTER = "__all__";
const string DistributeDocumentProcessor::FIELD_NAME_SEP = ";";
const string DistributeDocumentProcessor::KEY_VALUE_SEP = ":";
const string DistributeDocumentProcessor::CLUSTERS_SEP = ",";

// example:
// distribute_rule [field_value1: cluster1,cluster2; field_value2: cluster3]
// default_cluster [cluster1, cluster2]
bool DistributeDocumentProcessor::init(const DocProcessorInitParam &param) {
    _distributeFieldName = getValueFromKeyValueMap(param.parameters, DISTRIBUTE_FIELD_NAMES);
    if (_distributeFieldName.empty()) {
        string errorMsg = "distribute_field is empty";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    string ruleStr = getValueFromKeyValueMap(param.parameters, DISTRIBUTE_RULE);
    if (ruleStr.empty()) {
        string errorMsg = "distribute_rule is empty";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    vector<string> fieldValueRules = StringTokenizer::tokenize(
            ConstString(ruleStr), FIELD_NAME_SEP);
    for (size_t i = 0; i < fieldValueRules.size(); i++) {
        vector<string> keyValue = StringTokenizer::tokenize(
                ConstString(fieldValueRules[i]), KEY_VALUE_SEP);
        if (keyValue.size() != 2) {
            string errorMsg = "distribute_rule[" + ruleStr + "] is invalid";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        vector<string> ruleClusters = StringTokenizer::tokenize(
                ConstString(keyValue[1]), CLUSTERS_SEP);
        for (size_t clusterId = 0; clusterId < ruleClusters.size(); ++clusterId) {
            if (find(param.clusterNames.begin(), param.clusterNames.end(),
                     ruleClusters[clusterId]) == param.clusterNames.end()) {
                string errorMsg = "invalid cluster[" +ruleClusters[clusterId]
                                  + "] in distribute_rule[" + ruleStr + "]";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
        }
        _rule[keyValue[0]] = ruleClusters;
    }

    const string &defaultStr = getValueFromKeyValueMap(param.parameters, DEFAULT_CLUSTERS);
    vector<string> defaultClusters = StringTokenizer::tokenize(ConstString(defaultStr), CLUSTERS_SEP);
    if (defaultClusters.empty()) {
        BS_LOG(INFO, "default_clusters is empty");
    }

    const string &otherStr = getValueFromKeyValueMap(param.parameters, OTHER_CLUSTERS);
    _otherClusters = StringTokenizer::tokenize(ConstString(otherStr), CLUSTERS_SEP);
    if (_otherClusters.empty()) {
        _otherClusters = defaultClusters;
        BS_LOG(INFO, "other_clusters is empty, use default[%s]", defaultStr.c_str());
    }

    const string &emptyStr = getValueFromKeyValueMap(param.parameters, EMPTY_CLUSTERS);
    if (emptyStr.empty()) {
        BS_LOG(INFO, "empty_clusters empty, drop all doc without distribute field");
    } else if (ALL_CLUSTER == emptyStr) {
        BS_LOG(INFO, "empty_clusters is __all__, broadcast");
        _emptyClusters = param.clusterNames;
    } else {
        _emptyClusters = StringTokenizer::tokenize(ConstString(emptyStr), CLUSTERS_SEP);
    }

    return true;
}

void DistributeDocumentProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

bool DistributeDocumentProcessor::process(const ExtendDocumentPtr& document)
{
    const ProcessedDocumentPtr &processedDocPtr = document->getProcessedDocument();
    const RawDocumentPtr &rawDocCumentPtr = document->getRawDocument();

    const string &distributeValue = rawDocCumentPtr->getField(_distributeFieldName);

    const vector<string> *fittedClusters = NULL;
    // when disribute value is empty
    if (distributeValue.empty()) {
        fittedClusters = &_emptyClusters;
    } else {
        map<string, vector<string> >::const_iterator it = _rule.find(distributeValue);
        if (it != _rule.end()) {
            // rule found
            fittedClusters = &it->second;
        } else {
            // rule not found, go to other
            fittedClusters = &_otherClusters;
        }
    }

    ProcessedDocument::DocClusterMetaVec metas = processedDocPtr->getDocClusterMetaVec();

    if (fittedClusters->size() > 0) {
        // find fitted cluster.
        size_t leftCount = 0;
        for (size_t i = 0; i < metas.size(); i++) {
            if (find(fittedClusters->begin(), fittedClusters->end(),
                     metas[i].clusterName) != fittedClusters->end())
            {
                metas[leftCount++] = metas[i];
            }
        }
        metas.assign(metas.begin(), metas.begin() + leftCount);
    }

    if (fittedClusters->empty() || metas.size() == 0) {
        // skip useless document
        rawDocCumentPtr->setDocOperateType(SKIP_DOC);
    } else {
        // reset cluster metas
        processedDocPtr->setDocClusterMetaVec(metas);
    }
    return true;
}

void DistributeDocumentProcessor::destroy()
{
    delete this;
}

}
}
