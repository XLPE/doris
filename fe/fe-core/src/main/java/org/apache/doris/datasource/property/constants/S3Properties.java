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

package org.apache.doris.datasource.property.constants;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.CredProviderTypePB;
import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB.Provider;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.credentials.CloudCredential;
import org.apache.doris.common.credentials.CloudCredentialWithEndpoint;
import org.apache.doris.common.credentials.DataLakeAWSCredentialsProvider;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.thrift.TCredProviderType;
import org.apache.doris.thrift.TS3StorageParam;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class S3Properties extends BaseProperties {

    public static final String S3_PREFIX = "s3.";
    public static final String S3_FS_PREFIX = "fs.s3";

    public static final String CREDENTIALS_PROVIDER = "s3.credentials.provider";
    public static final String ENDPOINT = "s3.endpoint";
    public static final String EXTERNAL_ENDPOINT = "s3.external_endpoint";
    public static final String REGION = "s3.region";
    public static final String ACCESS_KEY = "s3.access_key";
    public static final String SECRET_KEY = "s3.secret_key";
    public static final String SESSION_TOKEN = "s3.session_token";

    public static final String ROLE_ARN = "s3.role_arn";
    public static final String EXTERNAL_ID = "s3.external_id";

    public static final String MAX_CONNECTIONS = "s3.connection.maximum";
    public static final String REQUEST_TIMEOUT_MS = "s3.connection.request.timeout";
    public static final String CONNECTION_TIMEOUT_MS = "s3.connection.timeout";
    public static final String S3_PROVIDER = "S3";

    // required by storage policy
    public static final String ROOT_PATH = "s3.root.path";
    public static final String BUCKET = "s3.bucket";
    public static final String VALIDITY_CHECK = "s3_validity_check";
    public static final String PROVIDER = "provider";
    public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT);
    public static final List<String> TVF_REQUIRED_FIELDS = Arrays.asList(ACCESS_KEY, SECRET_KEY);
    public static final List<String> FS_KEYS = Arrays.asList(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN,
            ROOT_PATH, BUCKET, MAX_CONNECTIONS, REQUEST_TIMEOUT_MS, CONNECTION_TIMEOUT_MS);

    public static final List<String> PROVIDERS = Arrays.asList("COS", "OSS", "S3", "OBS", "BOS", "AZURE", "GCP", "TOS");

    public static final List<String> AWS_CREDENTIALS_PROVIDERS = Arrays.asList(
            DataLakeAWSCredentialsProvider.class.getName(),
            TemporaryAWSCredentialsProvider.class.getName(),
            SimpleAWSCredentialsProvider.class.getName(),
            EnvironmentVariableCredentialsProvider.class.getName(),
            SystemPropertiesCredentialsProvider.class.getName(),
            InstanceProfileCredentialsProvider.class.getName(),
            ProfileCredentialsProvider.class.getName(),
            WebIdentityTokenCredentialsProvider.class.getName(),
            IAMInstanceCredentialsProvider.class.getName());

    private static final Pattern IPV4_PORT_PATTERN = Pattern.compile("((?:\\d{1,3}\\.){3}\\d{1,3}:\\d{1,5})");

    public static Map<String, String> credentialToMap(CloudCredentialWithEndpoint credential) {
        Map<String, String> resMap = new HashMap<>();
        resMap.put(S3Properties.ENDPOINT, credential.getEndpoint());
        resMap.put(S3Properties.REGION, credential.getRegion());
        if (credential.isWhole()) {
            resMap.put(S3Properties.ACCESS_KEY, credential.getAccessKey());
            resMap.put(S3Properties.SECRET_KEY, credential.getSecretKey());
        }
        if (credential.isTemporary()) {
            resMap.put(S3Properties.SESSION_TOKEN, credential.getSessionToken());
        }
        return resMap;
    }

    public static class Env {
        public static final String PROPERTIES_PREFIX = "AWS";
        // required
        public static final String ENDPOINT = "AWS_ENDPOINT";
        public static final String REGION = "AWS_REGION";
        public static final String ACCESS_KEY = "AWS_ACCESS_KEY";
        public static final String SECRET_KEY = "AWS_SECRET_KEY";
        public static final String TOKEN = "AWS_TOKEN";
        // required by storage policy
        public static final String ROOT_PATH = "AWS_ROOT_PATH";
        public static final String BUCKET = "AWS_BUCKET";
        // optional
        public static final String MAX_CONNECTIONS = "AWS_MAX_CONNECTIONS";
        public static final String REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
        public static final String CONNECTION_TIMEOUT_MS = "AWS_CONNECTION_TIMEOUT_MS";
        public static final String DEFAULT_MAX_CONNECTIONS = "50";
        public static final String DEFAULT_REQUEST_TIMEOUT_MS = "3000";
        public static final String DEFAULT_CONNECTION_TIMEOUT_MS = "1000";
        public static final String NEED_OVERRIDE_ENDPOINT = "AWS_NEED_OVERRIDE_ENDPOINT";

        public static final String ROLE_ARN = "AWS_ROLE_ARN";
        public static final String EXTERNAL_ID = "AWS_EXTERNAL_ID";

        public static final List<String> REQUIRED_FIELDS = Arrays.asList(ENDPOINT);
        public static final List<String> FS_KEYS = Arrays.asList(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY, TOKEN,
                ROOT_PATH, BUCKET, MAX_CONNECTIONS, REQUEST_TIMEOUT_MS, CONNECTION_TIMEOUT_MS);
    }

    public static CloudCredential getCredential(Map<String, String> props) {
        return getCloudCredential(props, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    }

    public static CloudCredentialWithEndpoint getEnvironmentCredentialWithEndpoint(Map<String, String> props) {
        CloudCredential credential = getCloudCredential(props, Env.ACCESS_KEY, Env.SECRET_KEY,
                Env.TOKEN);
        if (!props.containsKey(Env.ENDPOINT)) {
            throw new IllegalArgumentException("Missing 'AWS_ENDPOINT' property. ");
        }
        String endpoint = props.get(Env.ENDPOINT);
        String region = props.getOrDefault(Env.REGION, S3Properties.getRegionOfEndpoint(endpoint));
        props.putIfAbsent(Env.REGION, PropertyConverter.checkRegion(endpoint, region, Env.REGION));
        return new CloudCredentialWithEndpoint(endpoint, region, credential);
    }

    public static String getRegionOfEndpoint(String endpoint) {
        if (IPV4_PORT_PATTERN.matcher(endpoint).find()) {
            // if endpoint contains '192.168.0.1:8999', return null region
            return null;
        }
        String[] endpointSplit = endpoint.replace("http://", "")
                .replace("https://", "")
                .split("\\.");
        if (endpointSplit.length < 2) {
            return null;
        }
        if (endpointSplit[0].contains("oss-")) {
            // compatible with the endpoint: oss-cn-bejing.aliyuncs.com
            return endpointSplit[0];
        }
        return endpointSplit[1];
    }

    public static Map<String, String> prefixToS3(Map<String, String> properties) {
        Map<String, String> s3Properties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(OssProperties.OSS_PREFIX)) {
                String s3Key = entry.getKey().replace(OssProperties.OSS_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            } else if (entry.getKey().startsWith(GCSProperties.GCS_PREFIX)) {
                String s3Key = entry.getKey().replace(GCSProperties.GCS_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            }  else if (entry.getKey().startsWith(CosProperties.COS_PREFIX)) {
                String s3Key = entry.getKey().replace(CosProperties.COS_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            } else if (entry.getKey().startsWith(ObsProperties.OBS_PREFIX)) {
                String s3Key = entry.getKey().replace(ObsProperties.OBS_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            } else if (entry.getKey().startsWith(MinioProperties.MINIO_PREFIX)) {
                String s3Key = entry.getKey().replace(MinioProperties.MINIO_PREFIX, S3Properties.S3_PREFIX);
                s3Properties.put(s3Key, entry.getValue());
            } else {
                s3Properties.put(entry.getKey(), entry.getValue());
            }
        }
        return s3Properties;
    }

    public static Map<String, String> requiredS3TVFProperties(Map<String, String> properties)
            throws AnalysisException {
        try {
            for (String field : S3Properties.TVF_REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        return properties;
    }

    private static void checkProvider(Map<String, String> properties) throws DdlException {
        if (properties.containsKey(PROVIDER)) {
            properties.put(PROVIDER, properties.get(PROVIDER).toUpperCase());
            // S3 Provider properties should be case insensitive.
            if (!PROVIDERS.stream().anyMatch(s -> s.equals(properties.get(PROVIDER).toUpperCase()))) {
                throw new DdlException("Provider must be one of OSS, OBS, AZURE, BOS, COS, S3, GCP");
            }
        }
    }

    public static void requiredS3Properties(Map<String, String> properties) throws DdlException {
        // Try to convert env properties to uniform properties
        // compatible with old version
        S3Properties.convertToStdProperties(properties);
        if (properties.containsKey(S3Properties.Env.ENDPOINT)
                    && !properties.containsKey(S3Properties.ENDPOINT)) {
            for (String field : S3Properties.Env.REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        } else {
            for (String field : S3Properties.REQUIRED_FIELDS) {
                checkRequiredProperty(properties, field);
            }
        }
        checkProvider(properties);
    }

    public static void requiredS3PingProperties(Map<String, String> properties) throws DdlException {
        requiredS3Properties(properties);
        checkRequiredProperty(properties, S3Properties.BUCKET);
    }

    public static void checkRequiredProperty(Map<String, String> properties, String propertyKey)
            throws DdlException {
        String value = properties.get(propertyKey);
        if (Strings.isNullOrEmpty(value)) {
            throw new DdlException("Missing [" + propertyKey + "] in properties.");
        }
    }

    public static void optionalS3Property(Map<String, String> properties) {
        properties.putIfAbsent(S3Properties.MAX_CONNECTIONS, S3Properties.Env.DEFAULT_MAX_CONNECTIONS);
        properties.putIfAbsent(S3Properties.REQUEST_TIMEOUT_MS, S3Properties.Env.DEFAULT_REQUEST_TIMEOUT_MS);
        properties.putIfAbsent(S3Properties.CONNECTION_TIMEOUT_MS, S3Properties.Env.DEFAULT_CONNECTION_TIMEOUT_MS);
        // compatible with old version
        properties.putIfAbsent(S3Properties.Env.MAX_CONNECTIONS, S3Properties.Env.DEFAULT_MAX_CONNECTIONS);
        properties.putIfAbsent(S3Properties.Env.REQUEST_TIMEOUT_MS, S3Properties.Env.DEFAULT_REQUEST_TIMEOUT_MS);
        properties.putIfAbsent(S3Properties.Env.CONNECTION_TIMEOUT_MS, S3Properties.Env.DEFAULT_CONNECTION_TIMEOUT_MS);
    }

    public static void convertToStdProperties(Map<String, String> properties) {
        if (properties.containsKey(S3Properties.Env.ENDPOINT)) {
            properties.putIfAbsent(S3Properties.ENDPOINT, properties.get(S3Properties.Env.ENDPOINT));
        }
        if (properties.containsKey(S3Properties.Env.REGION)) {
            properties.putIfAbsent(S3Properties.REGION, properties.get(S3Properties.Env.REGION));
        }
        if (properties.containsKey(S3Properties.Env.ACCESS_KEY)) {
            properties.putIfAbsent(S3Properties.ACCESS_KEY, properties.get(S3Properties.Env.ACCESS_KEY));
        }
        if (properties.containsKey(S3Properties.Env.SECRET_KEY)) {
            properties.putIfAbsent(S3Properties.SECRET_KEY, properties.get(S3Properties.Env.SECRET_KEY));
        }
        if (properties.containsKey(S3Properties.Env.TOKEN)) {
            properties.putIfAbsent(S3Properties.SESSION_TOKEN, properties.get(S3Properties.Env.TOKEN));
        }
        if (properties.containsKey(S3Properties.Env.MAX_CONNECTIONS)) {
            properties.putIfAbsent(S3Properties.MAX_CONNECTIONS, properties.get(S3Properties.Env.MAX_CONNECTIONS));
        }
        if (properties.containsKey(S3Properties.Env.REQUEST_TIMEOUT_MS)) {
            properties.putIfAbsent(S3Properties.REQUEST_TIMEOUT_MS,
                    properties.get(S3Properties.Env.REQUEST_TIMEOUT_MS));

        }
        if (properties.containsKey(S3Properties.Env.CONNECTION_TIMEOUT_MS)) {
            properties.putIfAbsent(S3Properties.CONNECTION_TIMEOUT_MS,
                    properties.get(S3Properties.Env.CONNECTION_TIMEOUT_MS));
        }
        if (properties.containsKey(S3Properties.Env.ROOT_PATH)) {
            properties.putIfAbsent(S3Properties.ROOT_PATH, properties.get(S3Properties.Env.ROOT_PATH));
        }
        if (properties.containsKey(S3Properties.Env.BUCKET)) {
            properties.putIfAbsent(S3Properties.BUCKET, properties.get(S3Properties.Env.BUCKET));
        }
        if (properties.containsKey(PropertyConverter.USE_PATH_STYLE)) {
            properties.putIfAbsent(PropertyConverter.USE_PATH_STYLE, properties.get(PropertyConverter.USE_PATH_STYLE));
        }

        if (properties.containsKey(S3Properties.Env.ROLE_ARN)) {
            properties.putIfAbsent(S3Properties.ROLE_ARN, properties.get(S3Properties.Env.ROLE_ARN));
        }

        if (properties.containsKey(S3Properties.Env.EXTERNAL_ID)) {
            properties.putIfAbsent(S3Properties.EXTERNAL_ID, properties.get(S3Properties.Env.EXTERNAL_ID));
        }
    }

    public static TS3StorageParam getS3TStorageParam(Map<String, String> properties) {
        TS3StorageParam s3Info = new TS3StorageParam();

        if (properties.containsKey(S3Properties.ROLE_ARN)) {
            s3Info.setRoleArn(properties.get(S3Properties.ROLE_ARN));
            if (properties.containsKey(S3Properties.EXTERNAL_ID)) {
                s3Info.setExternalId(properties.get(S3Properties.EXTERNAL_ID));
            }
            s3Info.setCredProviderType(TCredProviderType.INSTANCE_PROFILE);
        }

        s3Info.setEndpoint(properties.get(S3Properties.ENDPOINT));
        s3Info.setRegion(properties.get(S3Properties.REGION));
        s3Info.setAk(properties.get(S3Properties.ACCESS_KEY));
        s3Info.setSk(properties.get(S3Properties.SECRET_KEY));
        s3Info.setToken(properties.get(S3Properties.SESSION_TOKEN));

        s3Info.setRootPath(properties.get(S3Properties.ROOT_PATH));
        s3Info.setBucket(properties.get(S3Properties.BUCKET));
        String maxConnections = properties.get(S3Properties.MAX_CONNECTIONS);
        s3Info.setMaxConn(Integer.parseInt(maxConnections == null
                ? S3Properties.Env.DEFAULT_MAX_CONNECTIONS : maxConnections));
        String requestTimeoutMs = properties.get(S3Properties.REQUEST_TIMEOUT_MS);
        s3Info.setRequestTimeoutMs(Integer.parseInt(requestTimeoutMs == null
                ? S3Properties.Env.DEFAULT_REQUEST_TIMEOUT_MS : requestTimeoutMs));
        String connTimeoutMs = properties.get(S3Properties.CONNECTION_TIMEOUT_MS);
        s3Info.setConnTimeoutMs(Integer.parseInt(connTimeoutMs == null
                ? S3Properties.Env.DEFAULT_CONNECTION_TIMEOUT_MS : connTimeoutMs));
        String usePathStyle = properties.getOrDefault(PropertyConverter.USE_PATH_STYLE, "false");
        s3Info.setUsePathStyle(Boolean.parseBoolean(usePathStyle));
        return s3Info;
    }

    public static Cloud.ObjectStoreInfoPB.Builder getObjStoreInfoPB(Map<String, String> properties) {
        Cloud.ObjectStoreInfoPB.Builder builder = Cloud.ObjectStoreInfoPB.newBuilder();
        if (properties.containsKey(S3Properties.ENDPOINT)) {
            builder.setEndpoint(properties.get(S3Properties.ENDPOINT));
        }
        if (properties.containsKey(S3Properties.REGION)) {
            builder.setRegion(properties.get(S3Properties.REGION));
        }
        if (properties.containsKey(S3Properties.ACCESS_KEY)) {
            builder.setAk(properties.get(S3Properties.ACCESS_KEY));
        }
        if (properties.containsKey(S3Properties.SECRET_KEY)) {
            builder.setSk(properties.get(S3Properties.SECRET_KEY));
        }
        if (properties.containsKey(S3Properties.ROOT_PATH)) {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(S3Properties.ROOT_PATH)),
                    "%s cannot be empty", S3Properties.ROOT_PATH);
            builder.setPrefix(properties.get(S3Properties.ROOT_PATH));
        }
        if (properties.containsKey(S3Properties.BUCKET)) {
            builder.setBucket(properties.get(S3Properties.BUCKET));
        }
        if (properties.containsKey(S3Properties.EXTERNAL_ENDPOINT)) {
            builder.setExternalEndpoint(properties.get(S3Properties.EXTERNAL_ENDPOINT));
        }
        if (properties.containsKey(S3Properties.PROVIDER)) {
            // S3 Provider properties should be case insensitive.
            builder.setProvider(Provider.valueOf(properties.get(S3Properties.PROVIDER).toUpperCase()));
        }

        if (properties.containsKey(PropertyConverter.USE_PATH_STYLE)) {
            String value = properties.get(PropertyConverter.USE_PATH_STYLE);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(value), "use_path_style cannot be empty");
            Preconditions.checkArgument(value.equalsIgnoreCase("true")
                    || value.equalsIgnoreCase("false"),
                    "Invalid use_path_style value: %s only 'true' or 'false' is acceptable", value);
            builder.setUsePathStyle(value.equalsIgnoreCase("true"));
        }

        if (properties.containsKey(S3Properties.ROLE_ARN)) {
            builder.setRoleArn(properties.get(S3Properties.ROLE_ARN));
            if (properties.containsKey(S3Properties.EXTERNAL_ID)) {
                builder.setExternalId(properties.get(S3Properties.EXTERNAL_ID));
            }
            builder.setCredProviderType(CredProviderTypePB.INSTANCE_PROFILE);
        }

        return builder;
    }
}
