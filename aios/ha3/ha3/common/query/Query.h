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
#include <ostream>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "suez/turing/expression/provider/common.h"

namespace isearch {
namespace common {

class ModifyQueryVisitor;
class Query;
class QueryVisitor;

typedef std::shared_ptr<Query> QueryPtr;

class Query {
public:
    typedef std::vector<QueryPtr> QueryVector;
    typedef std::vector<TermPtr> TermArray;

public:
    Query();
    Query(const Query &other);
    virtual ~Query();

public:
    virtual bool operator==(const Query &query) const = 0;
    virtual void accept(QueryVisitor *visitor) const = 0;
    virtual void accept(ModifyQueryVisitor *visitor) = 0;
    virtual Query *clone() const = 0;

    virtual void addQuery(QueryPtr queryPtr) {
        _children.push_back(queryPtr);
    }
    const std::vector<QueryPtr> *getChildQuery() const {
        return &_children;
    }
    std::vector<QueryPtr> *getChildQuery() {
        return &_children;
    }
    void setQueryRestrictor(bool restrictorFlag) {
        _queryRestrictor = restrictorFlag;
    }
    bool hasQueryRestrictor() const {
        return _queryRestrictor;
    }
    virtual std::string getQueryName() const = 0;
    virtual std::string toString() const;

    virtual void serialize(autil::DataBuffer &dataBuffer) const;
    virtual void deserialize(autil::DataBuffer &dataBuffer);

    virtual QueryType getType() const = 0;
    static Query *createQuery(QueryType type);
    const std::string &getQueryLabel() const {
        return _queryLabel;
    }
    void setQueryLabel(const std::string &label) {
        _queryLabel = label;
    }
    MatchDataLevel getMatchDataLevel() const {
        return _matchDataLevel;
    }
    void setMatchDataLevel(MatchDataLevel level) {
        _matchDataLevel = level;
    }
    virtual void setQueryLabelWithDefaultLevel(const std::string &label) = 0;

protected:
    void setQueryLabelBinary(const std::string &label);
    void setQueryLabelTerm(const std::string &label);
    void serializeMDLandQL(autil::DataBuffer &dataBuffer) const;
    void deserializeMDLandQL(autil::DataBuffer &dataBuffer);

protected:
    QueryVector _children;
    bool _queryRestrictor;
    MatchDataLevel _matchDataLevel;
    std::string _queryLabel;

private:
    AUTIL_LOG_DECLARE();
};

std::ostream &operator<<(std::ostream &out, const Query &query);

} // namespace common
} // namespace isearch

namespace autil {

template <>
inline void DataBuffer::write<isearch::common::Query>(isearch::common::Query const *const &p) {
    bool isNull = p;
    write(isNull);
    if (isNull) {
        QueryType type = p->getType();
        write(type);
        write(*p);
    }
}

template <>
inline void DataBuffer::read<isearch::common::Query>(isearch::common::Query *&p) {
    bool isNull;
    read(isNull);
    if (isNull) {
        QueryType type;
        read(type);
        p = isearch::common::Query::createQuery(type);
        if (p) {
            read(*p);
        }
    } else {
        p = NULL;
    }
}

} // namespace autil
