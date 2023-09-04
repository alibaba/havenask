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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"

namespace sql {

class Condition;
class ConditionVisitor;

typedef std::shared_ptr<Condition> ConditionPtr;
enum ConditionType {
    AND_CONDITION = 0,
    OR_CONDITION = 1,
    NOT_CONDITION = 2,
    LEAF_CONDITION = 3,
};

class Condition {
public:
    typedef std::vector<ConditionPtr> ConditionVector;

public:
    Condition();
    virtual ~Condition();

public:
    virtual ConditionType getType() const = 0;
    virtual void accept(ConditionVisitor *visitor) = 0;
    virtual std::string toString() = 0;

public:
    void addCondition(const ConditionPtr &conditionPtr) {
        _children.push_back(conditionPtr);
    }
    const std::vector<ConditionPtr> &getChildCondition() const {
        return _children;
    }
    const std::vector<ConditionPtr> &getChildCondition() {
        return _children;
    }
    void setTopJsonDoc(autil::AutilPoolAllocator *allocator, autil::SimpleDocument *topDoc);
    autil::SimpleDocument *getJsonDoc() {
        return _topDoc;
    }

public:
    static Condition *createCondition(ConditionType type);
    static Condition *createLeafCondition(const autil::SimpleValue &simpleVal);

protected:
    ConditionVector _children;
    autil::AutilPoolAllocator *_allocator;
    autil::SimpleDocument *_topDoc;
};

class AndCondition : public Condition {
public:
    void accept(ConditionVisitor *visitor) override;
    std::string toString() override;
    ConditionType getType() const override {
        return AND_CONDITION;
    }
};

class OrCondition : public Condition {
public:
    void accept(ConditionVisitor *visitor) override;
    std::string toString() override;
    ConditionType getType() const override {
        return OR_CONDITION;
    }
};

class NotCondition : public Condition {
public:
    void accept(ConditionVisitor *visitor) override;
    std::string toString() override;
    ConditionType getType() const override {
        return NOT_CONDITION;
    }
};

class LeafCondition : public Condition {
public:
    LeafCondition(const autil::SimpleValue &simpleVal);

public:
    void accept(ConditionVisitor *visitor) override;
    std::string toString() override;
    ConditionType getType() const override {
        return LEAF_CONDITION;
    }

public:
    const autil::SimpleValue &getCondition() {
        return _condition;
    }

private:
    const autil::SimpleValue &_condition;
};

} // namespace sql
