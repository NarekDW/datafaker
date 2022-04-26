package net.datafaker;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Strings.isNullOrEmpty;

public class OscarMovieTest extends AbstractFakerTest {

    @RepeatedTest(100)
    public void actor() {
        assertThat(faker.oscarMovie().actor()).matches("[\\p{L} .()-]+");
    }

    @Test
    public void movieName() {
        assertThat(isNullOrEmpty(faker.oscarMovie().movieName())).isFalse();
    }

    @Test
    public void quote() {
        assertThat(isNullOrEmpty(faker.oscarMovie().quote())).isFalse();
    }

    @RepeatedTest(100)
    public void character() {
        assertThat(faker.oscarMovie().actor()).matches("[\\p{L} .()-]+");
    }

    @RepeatedTest(100)
    public void releaseDate() {
        assertThat(faker.oscarMovie().releaseDate()).matches("[A-Za-z,0-9 ]+");
    }
}