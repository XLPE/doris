// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/exec/format/table/iceberg/partition_spec_parser.h"

#include <rapidjson/error/en.h>

#include "common/exception.h"

namespace doris::iceberg {
#include "common/compile_check_begin.h"
const char* PartitionSpecParser::SPEC_ID = "spec-id";
const char* PartitionSpecParser::FIELDS = "fields";
const char* PartitionSpecParser::SOURCE_ID = "source-id";
const char* PartitionSpecParser::FIELD_ID = "field-id";
const char* PartitionSpecParser::TRANSFORM = "transform";
const char* PartitionSpecParser::NAME = "name";

std::unique_ptr<PartitionSpec> PartitionSpecParser::from_json(const std::shared_ptr<Schema>& schema,
                                                              const std::string& json) {
    rapidjson::Document doc;
    doc.Parse(json.c_str());

    if (doc.HasParseError()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Failed to parse json: {}",
                               std::string(GetParseError_En(doc.GetParseError())));
    }
    int spec_id = doc[SPEC_ID].GetInt();
    UnboundPartitionSpec::Builder builder;
    builder.with_spec_id(spec_id);
    _build_from_json_fields(builder, doc[FIELDS]);
    return builder.build()->bind(schema);
}

void PartitionSpecParser::_build_from_json_fields(UnboundPartitionSpec::Builder& builder,
                                                  const rapidjson::Value& value) {
    DCHECK(value.IsArray());
    for (const auto& element : value.GetArray()) {
        DCHECK(element.IsObject());
        std::string name = element[NAME].GetString();
        std::string transform = element[TRANSFORM].GetString();
        int source_id = element[SOURCE_ID].GetInt();

        if (element.HasMember(FIELD_ID)) {
            builder.add_field(transform, source_id, element[FIELD_ID].GetInt(), name);
        } else {
            builder.add_field(transform, source_id, name);
        }
    }
}

#include "common/compile_check_end.h"
} // namespace doris::iceberg
