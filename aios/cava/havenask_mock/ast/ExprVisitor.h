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


namespace cava {

class AmbExpr;
class CallExpr;
class ConditionalExpr;
class FieldAccessExpr;
class LiteralExpr;
class NewArrayExpr;
class NewExpr;
class UnaryOpExpr;
class CastExpr;
class LocalDeclRefExpr;
class MemberExpr;

class ExprVisitor
{
public:
    ExprVisitor() : _error(false) {}
    virtual ~ExprVisitor() {}
private:
    ExprVisitor(const ExprVisitor &);
    ExprVisitor& operator=(const ExprVisitor &);
public:
    bool hasError() { return _error; }
protected:
    bool _error;
};

} // end namespace cava

