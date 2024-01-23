package org.ascension.addg.gcp.ingestion.core;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.common.io.ByteStreams;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;


/**
 * Provides a set of common static utility methods
 */
public final class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    private static final String COLUMN_NAME_REGEXP = "^[A-Za-z_]+\\w*$";

    public static Schema getErrorSchema(Schema.Options options) {
        var sb = Schema.builder()
                .addNullableStringField("errorMessage")
                .addNullableStringField("rawData");

        if (options != null) {
            sb = sb.setOptions(options);
        }

        return sb.build();
    }

    public static class SchemaExtractionException extends RuntimeException {
        public SchemaExtractionException(Throwable t) {
            super(t);
        }
    }

    /**
     * Prevents instantiation of this utility class
     */
    private Utils() {}

    /**
     * Reads data from the provided GCS path into a ValueProvider of String
     * @param gcsPath gs URL
     * @return file contents
     */
    public static ValueProvider<String> readFromGCS(ValueProvider<String> gcsPath) {
        return ValueProvider.NestedValueProvider.of(gcsPath, new SimpleFunction<>() {
            @Override
            public String apply(String input) {
                var sourceResourceId = FileSystems.matchNewResource(input, false);

                String schema;
                try (var rbc = FileSystems.open(sourceResourceId);
                     var baos = new ByteArrayOutputStream();
                     var wbc = Channels.newChannel(baos)) {
                        ByteStreams.copy(rbc, wbc);
                        schema = baos.toString(StandardCharsets.UTF_8);
                        LOG.info("Extracted data: {}", schema);
                } catch (IOException e) {
                    throw new Utils.SchemaExtractionException(e);
                }
                return schema;
            }
        });
    }

    /**
     * Cleans and validates a provided column name
     * @param column The column name to check
     * @param replacement Used to replace non-printable characters
     * @return The re-formatted column name
     */
    public static String checkHeaderName(String column, String replacement) {
        var checkedHeader = column
                .replaceAll("[^A-Za-z0-9]", replacement)
                .replaceAll("^(" + replacement + ")+", "")
                .replaceAll("(" + replacement + ")+$", "");
        if (!checkedHeader.matches(Utils.COLUMN_NAME_REGEXP)) {
            LOG.warn("Column name can't be matched to a valid format {}", column);
        }
        return checkedHeader.toLowerCase();
    }

    /**
     * Combines an Iterable of header Maps into a single Map
     */
    public static final SerializableFunction<Iterable<Map<String, Schema>>, Map<String, Schema>> combineHeaders = input -> {
        var finalMap = new LinkedHashMap<String, Schema>();

        Objects.requireNonNull(input).forEach(elem -> elem.forEach((key, value) ->
                finalMap.merge(key, value, (tables, schemas) -> schemas)
        ));

        return finalMap;
    };

    /**
     * Reads a structured json into a map
     * @param jsonNode Json node to flatten
     * @return flat map
     */
    @SuppressWarnings("unchecked")
    private static Object jsonToMap(JsonNode jsonNode, String headerReplacementChar) {
        Object retVal;

        if (jsonNode.isObject()) {
            retVal = new LinkedHashMap<>();
            var objectNode = (ObjectNode) jsonNode;
            var iter = objectNode.fields();

            while (iter.hasNext()) {
                var entry = iter.next();
                ((LinkedHashMap<String, Object>) retVal).put(Utils.checkHeaderName(entry.getKey(), headerReplacementChar), jsonToMap(entry.getValue(), headerReplacementChar));
            }

        } else if (jsonNode.isArray()) {
            var arrayNode = (ArrayNode) jsonNode;
            retVal = new ArrayList<>();
            for (var i = 0; i < arrayNode.size(); i++) {
                ((ArrayList<Object>) retVal).add(jsonToMap(arrayNode.get(i), headerReplacementChar));
            }

        } else {
            var valueNode = (ValueNode) jsonNode;
            retVal = valueNode.asText();
        }

        return retVal;
    }

    @SuppressWarnings("unchecked")
    private static Schema.FieldType getFieldType(Object o, String headerReplacementChar) {
        Schema.FieldType retVal;

        if (o instanceof Map) {
            var obj = (Map<String, Object>) o;

            // TODO: MAP key ordering is inconsistent
            if (!obj.isEmpty()) {
                var fv = obj.entrySet().iterator().next();
                retVal = Schema.FieldType.map(Schema.FieldType.STRING.withNullable(false), Utils.getFieldType(fv.getValue(), headerReplacementChar)).withNullable(true);
            } else {
                retVal = Schema.FieldType.map(Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING.withNullable(true)).withNullable(true);
            }

        } else if (o instanceof List) {
            var obj = (ArrayList<Object>) o;

            if (!obj.isEmpty()) {
                var fv = obj.get(0);
                retVal = Schema.FieldType.array(Utils.getFieldType(fv, headerReplacementChar)).withNullable(true);
            } else {
                retVal = Schema.FieldType.array(Schema.FieldType.STRING.withNullable(true)).withNullable(true);
            }

        } else {
            retVal = Schema.FieldType.STRING.withNullable(true);
        }

        return retVal;
    }

    /**
     * Converts a JsonNode to a Beam row
     * @param json JsonNode
     * @param options schema options
     * @param headerReplacementChar header replacement character
     * @return beam Row
     */
    @SuppressWarnings("unchecked")
    public static Row jsonToRow(JsonNode json, Schema.Options options, String headerReplacementChar) {
        var jsonMap = (Map<String, Object>) Utils.jsonToMap(json, headerReplacementChar);

        var sb = Schema.builder().setOptions(options);

        for (var e: jsonMap.entrySet()) {
            sb = sb.addNullableField(Utils.checkHeaderName(e.getKey(), headerReplacementChar), Utils.getFieldType(e.getValue(), headerReplacementChar));
        }

        return Row.withSchema(sb.build()).withFieldValues(jsonMap).build();
    }


    public static class CredentialsHelper {
        /**
         * Raised for errors when accessing credentials
         */
        public static class GetCredentialsException extends RuntimeException {
            public GetCredentialsException(Throwable e) {
                super(e);
            }
        }

        /**
         * Prevents instantiation of this class
         */
        private CredentialsHelper() {}

        /**
         * Retrieves a secret from Google Secrets Manager
         * @param projectId GCP Project id
         * @param secretId Secret id
         * @param versionId Version id
         * @return Secret data
         */
        public static String getSecret(String projectId, String secretId, String versionId) {

            try (var client = SecretManagerServiceClient.create()) {
                var secretVersionName = SecretVersionName.of(projectId, secretId, versionId);
                // Access the secret version.
                var response = client.accessSecretVersion(secretVersionName);
                return response.getPayload().getData().toStringUtf8();

            } catch (IOException e) {
                throw new Utils.CredentialsHelper.GetCredentialsException(e);
            }
        }

        /**
         * Retrieves the latest version of a secret from Google Secrets Manager
         * @param projectId GCP project id
         * @param secretId Secret id
         * @return Secret data
         */
        public static String getSecret(String projectId, String secretId) {
            return Utils.CredentialsHelper.getSecret(projectId, secretId, "latest");
        }
    }
}