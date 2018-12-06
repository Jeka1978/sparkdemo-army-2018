package taxi;

import static java.lang.Integer.parseInt;

/**
 * @author Evgeny Borisov
 */
public class TaxiUtil {
    public static taxi.TaxiOrder convertFromLine(String line) {
        String[] data = line.split(" ");
        return TaxiOrder.builder()
                .id(parseInt(data[0]))
                .city(data[1])
                .km(parseInt(data[2]))
                .build();
    }
}
