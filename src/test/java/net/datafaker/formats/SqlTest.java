package net.datafaker.formats;

import net.datafaker.providers.base.BaseFaker;
import net.datafaker.sequence.FakeSequence;
import net.datafaker.transformations.Schema;
import net.datafaker.transformations.SqlDialect;
import net.datafaker.transformations.SqlTransformer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static net.datafaker.transformations.Field.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.of;

public class SqlTest {
	@Test
	void testGenerateFromSchemaWithLimit() {
		BaseFaker faker = new BaseFaker(new Random(10L));
		Schema<Object, ?> schema = Schema.of(
				field("Text", () -> faker.name().firstName()),
				field("Bool", () -> faker.bool().bool())
		);
		
		SqlTransformer<Object> transformer = new SqlTransformer.SqlTransformerBuilder<>().build();
		String sql = transformer.generate(schema, 2);
		
		String expected = "INSERT INTO \"MyTable\" (\"Text\", \"Bool\") VALUES ('Willis', false);" +
				System.lineSeparator() +
				"INSERT INTO \"MyTable\" (\"Text\", \"Bool\") VALUES ('Carlena', true);";
		
		assertThat(sql).isEqualTo(expected);
	}
	
	@Test
	void testGenerateFromFakeSequenceCollection() {
		BaseFaker faker = new BaseFaker(new Random(10L));
		Schema<Integer, ?> schema = Schema.of(
				field("Number", integer -> integer),
				field("Password", integer -> faker.internet().password(integer, integer))
		);
		
		SqlTransformer<Integer> transformer = new SqlTransformer.SqlTransformerBuilder<Integer>().build();
		FakeSequence<Integer> fakeSequence = faker.<Integer>collection()
				.suppliers(() -> faker.number().randomDigit())
				.len(5)
				.build();
		
		String sql = transformer.generate(fakeSequence, schema);
		
		String expected = "INSERT INTO \"MyTable\" (\"Number\", \"Password\") VALUES (3, 'l63');" +
				System.lineSeparator() +
				"INSERT INTO \"MyTable\" (\"Number\", \"Password\") VALUES (6, 'z5s88e');" +
				System.lineSeparator() +
				"INSERT INTO \"MyTable\" (\"Number\", \"Password\") VALUES (7, '0b92c81');" +
				System.lineSeparator() +
				"INSERT INTO \"MyTable\" (\"Number\", \"Password\") VALUES (1, '5');" +
				System.lineSeparator() +
				"INSERT INTO \"MyTable\" (\"Number\", \"Password\") VALUES (3, 'zy2');";
		
		assertThat(sql).isEqualTo(expected);
	}
	
	@Test
	void testGenerateFromFakeSequenceStream() {
		BaseFaker faker = new BaseFaker(new Random(10L));
		Schema<Integer, ?> schema = Schema.of(
				field("Number", integer -> integer),
				field("Password", integer -> faker.internet().password(integer, integer))
		);
		
		SqlTransformer<Integer> transformer = new SqlTransformer.SqlTransformerBuilder<Integer>().build();
		FakeSequence<Integer> fakeSequence = faker.<Integer>stream()
				.suppliers(() -> faker.number().randomDigit())
				.len(2)
				.build();
		
		String sql = transformer.generate(fakeSequence, schema);
		
		String expected = "INSERT INTO \"MyTable\" (\"Number\", \"Password\") VALUES (3, 'f13');" +
				System.lineSeparator() +
				"INSERT INTO \"MyTable\" (\"Number\", \"Password\") VALUES (1, '5');";
		
		assertThat(sql).isEqualTo(expected);
	}
	
	@Test
	void testGenerateFromInfiniteFakeSequence() {
		BaseFaker faker = new BaseFaker(new Random(10L));
		Schema<Integer, ?> schema = Schema.of(
				field("Number", integer -> integer),
				field("Password", integer -> faker.internet().password(integer, integer))
		);
		
		SqlTransformer<Integer> transformer = new SqlTransformer.SqlTransformerBuilder<Integer>().build();
		FakeSequence<Integer> fakeSequence = faker.<Integer>stream()
				.suppliers(() -> faker.number().randomDigit())
				.build();
		
		assertThatThrownBy(() -> transformer.generate(fakeSequence, schema))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("The sequence should be finite of size");
	}
	
	@ParameterizedTest
    @MethodSource("generateTestSchema")
    void simpleSqlTestForSqlTransformer(Schema<String, String> schema, String expected) {
        SqlTransformer<String> transformer = new SqlTransformer.SqlTransformerBuilder<String>().sqlQuoteIdentifier("`").tableName("MY_TABLE").build();
        assertThat(transformer.generate(schema, 1)).isEqualTo(expected);
    }

    private static Stream<Arguments> generateTestSchema() {
        return Stream.of(
            of(Schema.of(), ""),
            of(Schema.of(field("key", () -> "value")), "INSERT INTO MY_TABLE (`key`) VALUES ('value');"),
            of(Schema.of(field("number", () -> 123)), "INSERT INTO MY_TABLE (`number`) VALUES (123);"),
            of(Schema.of(field("number", () -> 123.0)), "INSERT INTO MY_TABLE (`number`) VALUES (123.0);"),
            of(Schema.of(field("number", () -> 123.123)), "INSERT INTO MY_TABLE (`number`) VALUES (123.123);"),
            of(Schema.of(field("boolean", () -> true)), "INSERT INTO MY_TABLE (`boolean`) VALUES (true);"),
            of(Schema.of(field("nullValue", () -> null)), "INSERT INTO MY_TABLE (`nullValue`) VALUES (null);"));
    }

    @ParameterizedTest
    @MethodSource("generateTestSchemaForPostgres")
    void simpleSqlTestForSqlTransformerPostgres(Schema<String, String> schema, String expected) {
        SqlTransformer<String> transformer = new SqlTransformer.SqlTransformerBuilder<String>().dialect(SqlDialect.POSTGRES).build();
        assertThat(transformer.generate(schema, 1)).isEqualTo(expected);
    }

    private static Stream<Arguments> generateTestSchemaForPostgres() {
        return Stream.of(
            of(Schema.of(), ""),
            of(Schema.of(field("key", () -> "value")), "INSERT INTO \"MyTable\" (\"key\") VALUES ('value');"),
            of(Schema.of(field("number", () -> 123)), "INSERT INTO \"MyTable\" (\"number\") VALUES (123);"),
            of(Schema.of(field("number", () -> 123.0)), "INSERT INTO \"MyTable\" (\"number\") VALUES (123.0);"),
            of(Schema.of(field("number", () -> 123.123)), "INSERT INTO \"MyTable\" (\"number\") VALUES (123.123);"),
            of(Schema.of(field("boolean", () -> true)), "INSERT INTO \"MyTable\" (\"boolean\") VALUES (true);"),
            of(Schema.of(field("nullValue", () -> null)), "INSERT INTO \"MyTable\" (\"nullValue\") VALUES (null);"));
    }

    @ParameterizedTest
    @MethodSource("generateTestSchemaForMSSQL")
    void simpleSqlTestForSqlTransformerMSSQL(Schema<String, String> schema, String expected) {
        SqlTransformer<String> transformer = new SqlTransformer.SqlTransformerBuilder<String>().dialect(SqlDialect.MSSQL).build();
        assertThat(transformer.generate(schema, 1)).isEqualTo(expected);
    }

    private static Stream<Arguments> generateTestSchemaForMSSQL() {
        return Stream.of(
            of(Schema.of(), ""),
            of(Schema.of(field("key", () -> "value")), "INSERT INTO [MyTable] ([key]) VALUES ('value');"),
            of(Schema.of(field("number", () -> 123)), "INSERT INTO [MyTable] ([number]) VALUES (123);"),
            of(Schema.of(field("number", () -> 123.0)), "INSERT INTO [MyTable] ([number]) VALUES (123.0);"),
            of(Schema.of(field("number", () -> 123.123)), "INSERT INTO [MyTable] ([number]) VALUES (123.123);"),
            of(Schema.of(field("boolean", () -> true)), "INSERT INTO [MyTable] ([boolean]) VALUES (true);"),
            of(Schema.of(field("nullValue", () -> null)), "INSERT INTO [MyTable] ([nullValue]) VALUES (null);"));
    }
}
