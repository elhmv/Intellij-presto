/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.saphana;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_8;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.List;
//import static java.util.Locale.ENGLISH;
//import com.fasterxml.jackson.core.JsonFactoryBuilder;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.ImmutableMap;
//import io.airlift.slice.Slices;
//import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
//import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
//import io.prestosql.plugin.jdbc.expression.ImplementAvgDecimal;
//import io.prestosql.plugin.jdbc.expression.ImplementAvgFloatingPoint;
//import io.prestosql.plugin.jdbc.expression.ImplementCount;
//import io.prestosql.plugin.jdbc.expression.ImplementCountAll;
//import io.prestosql.plugin.jdbc.expression.ImplementMinMax;
//import io.prestosql.plugin.jdbc.expression.ImplementSum;
//import io.prestosql.plugin.postgresql.PostgreSqlConfig.ArrayMapping;
//import io.prestosql.spi.block.Block;
//import io.prestosql.spi.block.BlockBuilder;
//import io.prestosql.spi.block.SingleMapBlock;
//import io.prestosql.spi.connector.AggregateFunction;
//import io.prestosql.spi.connector.ColumnHandle;
//import io.prestosql.spi.connector.ConnectorTableMetadata;
//import io.prestosql.spi.connector.SchemaTableName;
//import io.prestosql.spi.connector.TableNotFoundException;
//import org.postgresql.core.TypeInfo;
//import org.postgresql.jdbc.PgConnection;
//import org.postgresql.util.PGobject;
//import java.sql.Array;
//import java.sql.PreparedStatement;
//import java.sql.Timestamp;
//import java.time.LocalDateTime;
//import java.util.function.BiFunction;
//import static com.google.common.base.Preconditions.checkArgument;
//import static com.google.common.base.Verify.verify;
//import static com.google.common.collect.ImmutableSet.toImmutableSet;
//import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
//import static io.airlift.slice.Slices.wrappedLongArray;
//import static io.prestosql.plugin.jdbc.ColumnMapping.PUSHDOWN_AND_KEEP;
//import static io.prestosql.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
//import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
//import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
//import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
//import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
//import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
//import static io.prestosql.plugin.postgresql.HanaConfig.ArrayMapping.AS_ARRAY;
//import static io.prestosql.plugin.postgresql.HanaConfig.ArrayMapping.AS_JSON;
//import static io.prestosql.plugin.postgresql.HanaConfig.ArrayMapping.DISABLED;
//import static io.prestosql.plugin.postgresql.PostgreSqlSessionProperties.getArrayMapping;
//import static io.prestosql.plugin.postgresql.TypeUtils.arrayDepth;
//import static io.prestosql.plugin.postgresql.TypeUtils.getArrayElementPgTypeName;
//import static io.prestosql.plugin.postgresql.TypeUtils.getJdbcObjectArray;
//import static io.prestosql.plugin.postgresql.TypeUtils.toPgTimestamp;
//import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
//import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
//import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
//import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
//import static io.prestosql.spi.type.StandardTypes.JSON;
//import static io.prestosql.spi.type.TimeType.TIME;
//import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
//import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
//import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
//import static io.prestosql.spi.type.TypeSignature.mapType;
//import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
//import static java.lang.Math.min;
//import static java.sql.DatabaseMetaData.columnNoNulls;
//import static java.util.Collections.addAll;
//import static java.util.Locale.ENGLISH;

public class HanaClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(HanaClient.class);
    private final Type jsonType;
    private String[] tableTypes;
    private static final Set<String> INTERNAL_SCHEMAS = ImmutableSet.<String>builder()
            .add("sap_sb")
            .add("SYS ")
            .add("_SYS_AUDIT")
            .add("SYSTEM")
            .add("SYS_REPL")
            .add("_SYS_SECURITY")
            .add("SYS_STREAMING")
            .add("SYS_XS_SBSS")
            .build();

    @Inject
    public HanaClient(
            BaseJdbcConfig config,
            HanaConfig hanaConfig,
            ConnectionFactory connectionFactory,
            TypeManager typeManager)
    {
        super(config, "\"", connectionFactory);
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        try (ResultSet resultSet = connection.getMetaData().getSchemas(connection.getCatalog(), null)) {
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                schemaNames.add(schemaName);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return schemaNames.build();
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                escapeNamePattern(schemaName, metadata.getSearchStringEscape()).orElse(null),
                escapeNamePattern(tableName, metadata.getSearchStringEscape()).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        Optional<ColumnMapping> unsignedMapping = getUnsignedMapping(typeHandle);
        if (unsignedMapping.isPresent()) {
            return unsignedMapping;
        }

        if (jdbcTypeName.equalsIgnoreCase("json")) {
            return Optional.of(jsonColumnMapping());
        }

        int columnSize = typeHandle.getColumnSize();
        switch (typeHandle.getJdbcType()) {
            // TODO not all these type constants are necessarily used by the JDBC driver
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                VarcharType varcharType = (columnSize <= VarcharType.MAX_LENGTH) ? createVarcharType(columnSize) : createUnboundedVarcharType();
                // Remote database can be case insensitive.
                return Optional.of(ColumnMapping.sliceMapping(varcharType, varcharReadFunction(), varcharWriteFunction()));

            case Types.DECIMAL:
                int precision = columnSize;
        }

        // TODO add explicit mappings
        return super.toPrestoType(session, connection, typeHandle);
    }

    private static Optional<ColumnMapping> getUnsignedMapping(JdbcTypeHandle typeHandle)
    {
        if (typeHandle.getJdbcTypeName().isEmpty()) {
            return Optional.empty();
        }

        String typeName = typeHandle.getJdbcTypeName().get();
        if (typeName.equalsIgnoreCase("tinyint unsigned")) {
            return Optional.of(smallintColumnMapping());
        }
        else if (typeName.equalsIgnoreCase("smallint unsigned")) {
            return Optional.of(integerColumnMapping());
        }
        else if (typeName.equalsIgnoreCase("int unsigned")) {
            return Optional.of(bigintColumnMapping());
        }
        else if (typeName.equalsIgnoreCase("bigint unsigned")) {
            return Optional.of(decimalColumnMapping(createDecimalType(20), UNNECESSARY));
        }

        return Optional.empty();
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static final ObjectMapper SORTED_MAPPER = new ObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

    private static Slice jsonParse(Slice slice)
    {
        try (JsonParser parser = createJsonParser(slice)) {
            byte[] in = slice.getBytes();
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(in.length);
            SORTED_MAPPER.writeValue((OutputStream) dynamicSliceOutput, SORTED_MAPPER.readValue(parser, Object.class));
            // nextToken() returns null if the input is parsed correctly,
            // but will throw an exception if there are trailing characters.
            parser.nextToken();
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert '%s' to JSON", slice.toStringUtf8()));
        }
    }

    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private static JsonParser createJsonParser(Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when using InputStream
        // so we pass an InputStreamReader instead.
        return JSON_FACTORY.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }
}
