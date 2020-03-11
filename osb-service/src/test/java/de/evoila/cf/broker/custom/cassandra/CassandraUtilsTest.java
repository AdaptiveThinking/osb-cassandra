package de.evoila.cf.broker.custom.cassandra;

import org.junit.jupiter.api.Test;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Johannes Strauß
 **/
class CassandraUtilsTest {

    private String validUuid = "8dc7110d-30b2-4553-8867-1777f8808cbf";

    @Test
    void throwsIllegalArgumentException() {
        String[] illegalInput = {null, "Invalid"};

        for (String uuid : illegalInput) {
            assertThrows(IllegalArgumentException.class, () -> CassandraUtils.dbName(uuid));
        }
    }

    @Test
    void returnsAlteredStringWhenGivenValidInput() {
        String result = CassandraUtils.dbName(validUuid);
        assertTrue(Pattern.matches("d[a-z0-9]{1,15}", result));
    }
}
