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
#ifndef ISEARCH_MULTI_CALL_GENERATORBASE_H
#define ISEARCH_MULTI_CALL_GENERATORBASE_H

#include "aios/network/gig/multi_call/common/Spec.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/util/MiscUtil.h"

namespace multi_call {

class GeneratorBase
{
public:
    GeneratorBase(const std::string &clusterName, const std::string &bizName,
                  const std::string &strategy, const std::string &requestId, SourceIdTy sourceId,
                  VersionTy version, VersionTy preferVersion);
    ~GeneratorBase();

private:
    GeneratorBase(const GeneratorBase &);
    GeneratorBase &operator=(const GeneratorBase &);

public:
    void setClusterName(const std::string &clusterName) {
        _clusterName = clusterName;
    }
    void setBizName(const std::string &bizName) {
        _bizName = bizName;
    }
    std::string getBizName() const {
        return MiscUtil::createBizName(_clusterName, _bizName);
    }
    const std::string &getClusterName() const {
        return _clusterName;
    }
    void setFlowControlStrategy(const std::string &strategy) {
        _strategy = strategy;
    }
    const std::string &getFlowControlStrategy() const {
        if (!_strategy.empty()) {
            return _strategy;
        } else {
            return _bizName;
        }
    }
    void setRequestId(const std::string &requestId) {
        _requestId = requestId;
    }
    const std::string &getRequestId() const {
        return _requestId;
    }
    void setSourceId(SourceIdTy sourceId) {
        _sourceId = sourceId;
    }
    SourceIdTy getSourceId() const {
        return _sourceId;
    }
    VersionTy getVersion() const {
        return _version;
    }
    void setVersion(VersionTy version) {
        _version = version;
    }
    VersionTy getPreferVersion() const {
        return _preferVersion;
    }
    void setPreferVersion(VersionTy preferVersion) {
        _preferVersion = preferVersion;
    }
    void setUserRequestType(RequestType type) {
        _requestType = type;
    }
    RequestType getUserRequestType() const {
        return _requestType;
    }
    void setSpec(const Spec &spec) {
        _spec = spec;
    }
    const Spec &getSpec() const {
        return _spec;
    }
    void setDisableRetry(bool disable) {
        _disableRetry = disable;
    }
    bool getDisableRetry() const {
        return _disableRetry || _disableProbe;
    }
    void setDisableProbe(bool disable) {
        _disableProbe = disable;
    }
    bool getDisableProbe() const {
        return _disableProbe;
    }
    void setDisableDegrade(bool disable) {
        _disableDegrade = disable;
    }
    bool getDisableDegrade() const {
        return _disableDegrade;
    }
    void setIgnoreWeightLabelInConsistentHash(bool ignore) {
        _ignoreWeightLabelInConsistentHash = ignore;
    }
    bool getIgnoreWeightLabelInConsistentHash() const {
        return _ignoreWeightLabelInConsistentHash;
    }
    void addMatchTag(const std::string &tag, TagMatchType type) {
        if (!_tags) {
            _tags = std::make_shared<MatchTagMap>();
        }
        TagMatchInfo info;
        info.type = type;
        (*_tags)[tag] = info;
    }
    bool hasMatchTag(const std::string &tag) const {
        return _tags && (_tags->end() != _tags->find(tag));
    }
    void deleteMatchTag(const std::string &tag) {
        if (_tags) {
            _tags->erase(tag);
        }
    }
    void setMatchTags(const MatchTagMapPtr &tags) {
        _tags = tags;
    }
    MatchTagMapPtr cloneMatchTags() const {
        if (!_tags) {
            return nullptr;
        }
        return std::make_shared<MatchTagMap>(*_tags);
    }

protected:
    std::string _clusterName;
    std::string _bizName;
    std::string _strategy;
    std::string _requestId;
    SourceIdTy _sourceId;
    VersionTy _version;
    VersionTy _preferVersion;
    RequestType _requestType;
    Spec _spec;
    bool _disableRetry                      : 1;
    bool _disableProbe                      : 1;
    bool _disableDegrade                    : 1;
    bool _ignoreWeightLabelInConsistentHash : 1;
    MatchTagMapPtr _tags;
};

MULTI_CALL_TYPEDEF_PTR(GeneratorBase);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GENERATORBASE_H
